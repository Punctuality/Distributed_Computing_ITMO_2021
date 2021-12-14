//
// Created by Sergey Fedorov on 12/4/21.
//

#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <stdio.h>
#include <sched.h>
#include <fcntl.h>

#include "banking.h"
#include "ipc.h"
#include "pa2345.h"
#include "common.h"

int receive_blocking(void* self, local_id id, Message* msg);

typedef int pipe_fd;
typedef struct {
    pipe_fd in_pipe;
    pipe_fd out_pipe;
} channel;
typedef struct {
    // Ids
    pid_t pid;
    pid_t parent;

    // Global state info
    uint8_t p_count;
    local_id id;

    // IPC
    channel* channels;

    // We can't use shared memory, therefore...
    FILE* const log_file;

    // Task specific
    balance_t balance;
} process_info;

// FIXME Add defines for release functions masks

// FIXME maybe use getopt
static int process_args(const int argc, const char** argv, int* p_count, balance_t** balances) {
    char* endptr = "\0";

    int i;
    for(i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-p") == 0 && i + 1 < argc) {
            i++;
            *p_count = (int) strtol(argv[i], &endptr, 10) + 1;
            break;
        }
    }

    if (strcmp(endptr, "\0") != 0) {
        return -endptr[0];
    }

    i++;
    if ((argc - i) < (*p_count - 1)) {
        return 1;
    }

    *balances = calloc(*p_count - 1, sizeof(balance_t));
    if (!*balances) {
        return 2;
    }

    for(int j = 0; j < (*p_count - 1); j++, i++) {
        (*balances)[j] = (balance_t) strtol(argv[i], &endptr, 10);
        if (strcmp(endptr, "\0") != 0) {
            return -endptr[0];
        }
    }

    return 0;
}

void write_log(FILE* file, const char* format, ...) {
    va_list varargs;
    va_start(varargs, format);
    vfprintf(file, format, varargs);
    va_end(varargs);

    va_start(varargs, format);
    vprintf(format, varargs);
    va_end(varargs);

    fflush(stdout);
    fflush(file);
}

int release_pipes(int mask, ...) {
    va_list varargs;

    switch (mask) {
        case 0x00:
            va_start(varargs, mask);
            FILE* log_file0 = va_arg(varargs, FILE*);
            fclose(log_file0);
            va_end(varargs);
            break;
        case 0x01:
            va_start(varargs, mask);
            channel** channels1 = va_arg(varargs, channel**);

            free(*channels1);
            va_end(varargs);
            break;
        case 0x02:
            va_start(varargs, mask);
            int init_count = va_arg(varargs, int);
            channel** channels2 = va_arg(varargs, channel**);
            FILE* log_file2 = va_arg(varargs, FILE*);

            for (int i = 0; i < init_count; i++) {
                if ((*channels2)[i].in_pipe > 0) close((*channels2)[i].in_pipe);
                if ((*channels2)[i].out_pipe > 0) close((*channels2)[i].out_pipe);
            }

            fclose(log_file2);
            free(*channels2);
            va_end(varargs);
            break;
        default: break;
    }

    return -mask;
}

static int nonblocking_fd(int fd) {
    int cur_flag = fcntl(fd, F_GETFL);

    if (cur_flag == -1 || fcntl(fd, F_SETFL, cur_flag | O_NONBLOCK) != 0) {
        return -1;
    } else {
        return 0;
    }
}

int prepare_pipes(uint8_t p_count, channel** all_channels, FILE* const log_file) {
    uint32_t cross_p = p_count * p_count;
    *all_channels = calloc(cross_p, sizeof(channel));

    if (!*all_channels) {
        return release_pipes(0x01, all_channels);
    }

    for (int i = 0, k = 0; i < p_count; i++)
        for (int j = 0; j < p_count; j++, k++)
            if (i == j) continue;
            else {
                pipe_fd tuple[2];
                if (pipe(tuple) < 0) {
                    return release_pipes(0x02, k, all_channels, log_file);
                } else {
                    if (nonblocking_fd(tuple[0]) || nonblocking_fd(tuple[1])) {
                        return release_pipes(0x02, k, all_channels, log_file);
                    }

                    (*all_channels)[k].in_pipe = tuple[0];
                    (*all_channels)[k].out_pipe = tuple[1];

                    fprintf(log_file, "%d / %d\n", tuple[0], tuple[1]);
                }
            }

    return release_pipes(0x00, log_file);
}

void attach_pipes(process_info* proc, channel** all_channels) {
    for (int i = 0; i < proc->p_count; i++) {
        proc->channels[i].in_pipe = (*all_channels)[proc->id * proc->p_count + i].in_pipe;
        proc->channels[i].out_pipe = (*all_channels)[proc->id + proc->p_count * i].out_pipe;

        // marking these channels not to close them later
        (*all_channels)[proc->id * proc->p_count + i].in_pipe = -1;
        (*all_channels)[proc->id + proc->p_count * i].out_pipe = -1;
    }
}

int kill(pid_t pid, int sig);
int release_processes(int mask, ...) {
    va_list varargs;

    switch (mask) {
        case 0x00:
            va_start(varargs, mask);
            channel** channels0 = va_arg(varargs, channel**);
            int chan_len = va_arg(varargs, int);
            chan_len = chan_len * chan_len;
            for (int i = 0; i < chan_len; i++) {
                if ((*channels0)[i].in_pipe > 0)
                    close((*channels0)[i].in_pipe);
                if ((*channels0)[i].out_pipe > 0)
                    close((*channels0)[i].out_pipe);
            }
            free(*channels0);
            va_end(varargs);
            break;
        case 0x03:
            va_start(varargs, mask);
            pid_t** to_free3 = va_arg(varargs, pid_t**);
            FILE* to_close3 = va_arg(varargs, FILE*);
            int to_kill = va_arg(varargs, int);

            for (int i = 0; i < to_kill; i++)
                kill((*to_free3)[i], SIGKILL);
            free(*to_free3);
            fclose(to_close3);
            va_end(varargs);
            break;
        case 0x02:
            va_start(varargs, mask);
            pid_t** to_free2 = va_arg(varargs, pid_t**);
            FILE* to_close2 = va_arg(varargs, FILE*);
            free(*to_free2);
            fclose(to_close2);
            va_end(varargs);
            break;
        case 0x01:
            va_start(varargs, mask);
            pid_t** to_free1 = va_arg(varargs, pid_t**);
            free(*to_free1);
            va_end(varargs);
            break;
        default: break;
    }

    return mask;
}

int release_task(int mask, ...) {
    va_list varargs;

    switch (mask) {
        case 0x01:
        case 0x02:
        case 0x03:
        case 0x04:
            va_start(varargs, mask);
            channel** channels = va_arg(varargs, channel**);
            FILE* file = va_arg(varargs, FILE*);
            int count = va_arg(varargs, int);

            for (int i = 0; i < count; i++) {
                if ((*channels)[i].in_pipe > 0) close((*channels)[i].in_pipe);
                if ((*channels)[i].out_pipe > 0) close((*channels)[i].out_pipe);
            }
            fclose(file);
            va_end(varargs);
            break;
        default:
            break;
    }

    return -mask;
}

Message compose_message(process_info* proc, MessageType type, const char* format, ...) {
    Message msg;
    va_list varargs;
    va_start(varargs, format);
    msg.s_header.s_magic = MESSAGE_MAGIC;
    msg.s_header.s_type = type;
    msg.s_header.s_payload_len =
            vsnprintf(msg.s_payload, MAX_PAYLOAD_LEN, format, varargs);
    va_end(varargs);

    return msg;
}

int sync_states(process_info* proc, MessageType state) {
    for (local_id i = PARENT_ID + 1; i < proc->p_count; i++) {
        if (i == proc->id) continue;

        Message msg;
        if (receive_blocking(proc, i, &msg) > 0) {
            fprintf(stderr, "%d, Error on syncing %d\n", proc->id, state);
            return release_task(0x02, &proc->channels, proc->log_file, (int) proc->p_count);
        }
        if (msg.s_header.s_type != state) return 1;
    }

    return 0;
}

int task(process_info* proc) {

    write_log(proc->log_file,log_started_fmt,
            get_physical_time(), proc->id, proc->pid, proc->parent, proc->balance);

    Message start_message = compose_message(
            proc,
            STARTED,
            log_started_fmt,
            get_physical_time(), proc->id, proc->pid, proc->parent, proc->balance);

    if (send_multicast(proc, &start_message)) {
        fprintf(stderr, "%d, Error on sending STARTED\n", proc->id);
        return release_task(0x01, proc->channels, proc->log_file, (int) proc->p_count);
    }

    if (sync_states(proc, STARTED)) {
        fprintf(stderr, "%d, Error on syncing\n", proc->id);
        return 2;
    }

    write_log(proc->log_file, log_received_all_started_fmt, get_physical_time(), proc->id);

    int done = 1;

    BalanceHistory sub_history;
    sub_history.s_id = proc->id;
    sub_history.s_history_len = 0;

    int is_working = 1;
    while (is_working) {
        Message msg;


        // Receive any message
        if (receive_any(proc, &msg) != 0)
            return release_task(0x03, proc->channels, proc->log_file, (int) proc->p_count);

        const timestamp_t time = get_physical_time();
        // Fill balance history with timestamps
        for (timestamp_t i = sub_history.s_history_len; i < time; ++i) {
            sub_history.s_history[i].s_time = i;
            sub_history.s_history[i].s_balance = proc->balance;
            sub_history.s_history[i].s_balance_pending_in = 0;
        }
        sub_history.s_history_len = time;

        TransferOrder* transfer;
        switch (msg.s_header.s_type) {
            case STOP:
                // Current done is not checked?
                ++done;

                write_log(proc->log_file, log_done_fmt, get_physical_time(), proc->id, proc->balance);

                Message done_message = compose_message(
                        proc,
                        DONE,
                        log_done_fmt,
                        get_physical_time(), proc->id, proc->balance);

                if (send_multicast(proc, &done_message)) {
                    fprintf(stderr, "%d, Error on sending DONE\n", proc->id);
                    return release_task(0x04, proc->channels, proc->log_file, (int) proc->p_count);
                }
                break;

            case DONE:
                // Like syn_states but with state and non-blocking
                ++done;
                if (done == proc->p_count) {
                    is_working = 0;
                }
                break;

            case TRANSFER:
                transfer = (TransferOrder*) msg.s_payload;
                if (proc->id == transfer->s_src) {
                    proc->balance -= transfer->s_amount;

                    if (send(proc, transfer->s_dst, &msg) != 0) {
                        return release_processes(0x03, proc->channels, proc->log_file, (int) proc->p_count);
                    }

                    write_log(proc->log_file, log_transfer_out_fmt, time, proc->id, transfer->s_amount, transfer->s_dst);
                } else if (proc->id == transfer->s_dst) {
                    write_log(proc->log_file, log_transfer_in_fmt, time, proc->id, transfer->s_amount, transfer->s_src);

                    proc->balance += transfer->s_amount;

                    Message ack;
                    ack.s_header.s_magic = MESSAGE_MAGIC;
                    ack.s_header.s_type = ACK;
                    ack.s_header.s_payload_len = 0;

                    if (send(proc, PARENT_ID, &ack) != 0)
                        return release_processes(0x03, proc->channels, proc->log_file, (int) proc->p_count);
                }
                break;
        }
    }

    sub_history.s_history[sub_history.s_history_len].s_time = sub_history.s_history_len;
    sub_history.s_history[sub_history.s_history_len].s_balance = proc->balance;
    sub_history.s_history[sub_history.s_history_len].s_balance_pending_in = 0;
    sub_history.s_history_len++;

    Message msg;
    msg.s_header.s_magic = MESSAGE_MAGIC;
    msg.s_header.s_type = BALANCE_HISTORY;
    // offsetof = sizeof(1) + sizeof(2);
    msg.s_header.s_payload_len = sizeof(BalanceState) * sub_history.s_history_len + offsetof(BalanceHistory, s_history) ;
    memcpy(msg.s_payload, &sub_history, msg.s_header.s_payload_len);

    if (send(proc, PARENT_ID, &msg) != 0)
        return release_task(0x04, proc->channels, proc->log_file, (int) proc->p_count);

    write_log(proc->log_file, log_received_all_done_fmt, get_physical_time(), proc->id);

    return 0;
}

int start_processes(process_info* parent, pid_t** child_pids, const balance_t* balances, int(*child_func)(process_info*)) {
    *child_pids = calloc(parent->p_count, sizeof(pid_t));

    FILE* const events_file = fopen(events_log, "a+");
    if (events_file == NULL) {
        fprintf(stderr, "Failed to create events log file (%s)\n", events_log);
        return release_processes(0x01, child_pids);
    }

    channel* all_channels;
    FILE* const log_file = fopen(pipes_log, "w+");
    if (log_file == NULL || prepare_pipes(parent->p_count, &all_channels, log_file) > 0) {
        fprintf(stderr, "Failed to prepare pipes \n");
        return release_processes(0x02, child_pids, log_file);
    }

    for (int i = 1; i < parent->p_count; i++) {
        pid_t forked_pid = fork();

        if (forked_pid < 0) {
            release_processes(0x03, child_pids, log_file, i);
        } else if (forked_pid == 0) {
            process_info child_info = {
                    getpid(),
                    parent->pid,
                    parent->p_count,
                    (local_id) i,
                    calloc(parent->p_count, sizeof(channel)),
                    events_file,
                    balances[i-1]
            };

            attach_pipes(&child_info, &all_channels);
            release_processes(0x00, &all_channels, (int) child_info.p_count/*, child_info.id*/);
            exit(child_func(&child_info));
        }
    }

    attach_pipes(parent, &all_channels);
    release_processes(0x00, &all_channels, (int) parent->p_count/*, parent->id*/);
    return 0;
}

int join_all(process_info* proc, pid_t* sub_processes) {
    if (sync_states(proc, STARTED)) {
        fprintf(stderr, "%d, Error on syncing\n", proc->id);
        return 2;
    }

    bank_robbery(proc, (local_id) (proc->p_count - 1));

    Message msg;
    msg.s_header.s_magic = MESSAGE_MAGIC;
    msg.s_header.s_type = STOP;
    msg.s_header.s_payload_len = 0;

    if (send_multicast(proc, &msg) < 0) return 3;


    if (sync_states(proc, DONE)) {
        fprintf(stderr, "%d, Error on syncing\n", proc->id);
        return 4;
    }

    AllHistory history;
    history.s_history_len = proc->p_count - 1;

    for (local_id id = 1; (uint8_t) id < proc->p_count; id++) {
        Message data_msg;

        if (receive_blocking(proc, id, &data_msg) > 0)
            return 1;

        if (data_msg.s_header.s_type != BALANCE_HISTORY)
            return 1;

        BalanceHistory* sub_history = (BalanceHistory*) data_msg.s_payload;

        history.s_history[id-1].s_history_len = sub_history->s_history_len;
        history.s_history[id-1].s_id = sub_history->s_id;

        for (size_t i = 0; i < sub_history->s_history_len; i++) {
            history.s_history[id-1].s_history[i] = sub_history->s_history[i];
        }

    }

    print_history(&history);

    for (int i = 1; i < proc->p_count; i++) {
        if (waitpid(sub_processes[i], NULL, 0) < 0) {
            if (errno == ECHILD) {
                return 0;
            } else {
                return 1;
            }
        }
    }

    return 0;
}

int main(int argc, char* argv[]) {

    int process_count;
    balance_t* balances;

    // Parse arguments (-p X a b c d ...)
    int args_res = process_args(argc, (const char **) argv, &process_count, &balances);

    if(args_res != 0) {
        fprintf(stderr, "Failed to parse args");
        if (args_res <= -1) {
            fprintf(stderr, " (encountered a non-digit: '%c')\n", (char) -args_res);
        } else if (args_res == 1) {
            fprintf(stderr, " (not enough arguments)\n");
        } else if (args_res == 2) {
            fprintf(stderr, " (unable to allocate balances)\n");
        } else fprintf(stderr, "\n");
        exit(1);
    }

    // Start the processes
    process_info parent = {
            getpid(),
            getpid(),
            (uint8_t) process_count,
            PARENT_ID,
            calloc(process_count, sizeof(channel)),
            NULL
    };
    pid_t* child_pids;
    if (start_processes(&parent, &child_pids, balances, task)) {
        fprintf(stderr, "Failed to create child-processes\n");
        free(balances);
        return 1;
    }

    // Wait for the processes to finish
    if (join_all(&parent, child_pids)) {
        fprintf(stderr, "Failed to join child-processes\n");
        free(balances);
        free(child_pids);
        return 2;
    }

    // Exit
    free(parent.channels);
    free(balances);
    free(child_pids);
    return 0;
}

static int send_all(int fd, const void* buf, size_t left){
    const char* ptr = buf;
    ssize_t sent;

    errno = 0;
    for (;;) {
        if ((sent = write(fd, ptr, left)) < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                sched_yield();
                continue;
            }

            fprintf(stderr, "Encountered a sending problem %s\n", strerror(errno));
            break;
        }

        left -= sent; ptr += sent;

        if (left == 0)
            return (int) sent;
    }

    return -1;
}

static size_t msg_size(const Message* msg) {
    return sizeof(MessageHeader) + msg->s_header.s_payload_len;
}

int send(void* self, local_id dst, const Message* msg){
    process_info* proc = self;

    if (send_all(proc->channels[dst].out_pipe, msg, msg_size(msg)) < 0)
        return 1;

    return 0;
}

int send_multicast(void* self, const Message* msg){
    process_info* proc = self;

    for (local_id i = 0; i < proc->p_count; i++) {
        if (i == proc->id) continue;

        if (send(proc, i, msg))
            return 1;
    }

    return 0;
}

static int receive_full(int fd, void* buf, size_t left) {
    char* ptr = buf;
    ssize_t bytes_read;

    errno = 0;
    for (;;) {
        if ((bytes_read = read(fd, ptr, left)) < 0) {
            if (ptr != buf && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                sched_yield();
                continue;
            }

            if (!(errno == EAGAIN || errno == EWOULDBLOCK)) {
                fprintf(stderr, "Encountered a receiving problem %d %s\n", errno, strerror(errno));
            }
            break;
        }

        left -= bytes_read; ptr += bytes_read;

        if (left == 0)
            return (int) bytes_read;

        if (bytes_read == 0) {
            if (errno == 0) {
                errno = EAGAIN;
            }

            break;
        }

    }

    return -1;
}

int receive(void* self, local_id from, Message* msg){
    process_info* proc = self;

    // Non-blocking ask for header
    if (receive_full(proc->channels[from].in_pipe, &(msg->s_header), sizeof(MessageHeader)) < 0) {
        return 1;
    }

    // Blocking ask for finishing body
    while (receive_full(proc->channels[from].in_pipe, msg->s_payload, msg->s_header.s_payload_len) < 0) {
        if (errno == EWOULDBLOCK || errno == EAGAIN) {
            sched_yield();
            continue;
        }

        return 2;
    }

    return 0;
}

int receive_blocking(void* self, local_id id, Message * msg) {
//    process_info* proc = self;

    for (;;) {
        int ret = receive( self, id, msg);

        if (ret != 0 && (errno == EWOULDBLOCK || errno == EAGAIN)){
            sched_yield();
            continue;
        }

        return ret;
    }
}

int receive_any(void* self, Message* msg){
    process_info* proc = self;

    for(;;) {
        for (local_id i = 0; i < proc->p_count; i++) {
            if (i == proc->id) continue;

            if (!receive(proc, i, msg)) return 0;

            if (errno != EAGAIN || errno != EWOULDBLOCK) {
                fprintf(stderr, "ERROR in receive_any %s\n", strerror(errno));
                return -1;
            }

        }
        sched_yield();
    }

}

void transfer(void* parent_data, local_id src, local_id dst, balance_t amount) {
    process_info* proc = parent_data;

    if (proc->id == PARENT_ID) {
        Message msg;

        msg.s_header.s_magic = MESSAGE_MAGIC;
        msg.s_header.s_type = TRANSFER;
        msg.s_header.s_payload_len = sizeof(TransferOrder);

        TransferOrder* transfer = (TransferOrder *) msg.s_payload;
        transfer->s_src = src;
        transfer->s_dst = dst;
        transfer->s_amount = amount;

        send(parent_data, src, &msg);

        receive_blocking(parent_data, dst, &msg);

        if (msg.s_header.s_type != ACK) {
            fprintf(stderr, "Received non ACK type on confirmation (from %d)\n", dst);
            exit(1);
        }

    } else {
        fprintf(stderr, "Transfer was called by child %d\n", proc->id);
        exit(1);
    }
}

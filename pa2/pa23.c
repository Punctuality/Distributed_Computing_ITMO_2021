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

#include "banking.h"
#include "ipc.h"
#include "pa2345.h"
#include "common.h"

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
int get_process_count(const int argc, const char** argv, int* p_count, balance_t** balances) {
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
                // TODO Add making non-block
                if (pipe(tuple) < 0) {
                    return release_pipes(0x02, k, all_channels, log_file);
                } else {
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
        // marking these channels not to close
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
        // TODO Add receive blocking
        if (receive(proc, i, &msg) < 0) {
            fprintf(stderr, "%d, Error on syncing %d\n", proc->id, state);
            return release_task(0x02, &proc->channels, proc->log_file, (int) proc->p_count);
        }
        if (msg.s_header.s_type != state) return 1;
    }

    return 0;
}

void work(){}

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

    work();

    write_log(proc->log_file, log_done_fmt, proc->id);

    Message done_message = compose_message(
            proc,
            DONE,
            log_done_fmt,
            proc->id, proc->pid, proc->parent);

    if (send_multicast(proc, &done_message)) {
        fprintf(stderr, "%d, Error on sending DONE\n", proc->id);
        return release_task(0x03, proc->channels, proc->log_file, (int) proc->p_count);
    }

    if (sync_states(proc, DONE)) {
        fprintf(stderr, "%d, Error on syncing\n", proc->id);
        return 4;
    }

    write_log(proc->log_file, log_received_all_done_fmt, proc->id);

    return 0;
}

int start_processes(process_info* parent, pid_t** child_pids,  int(*child_func)(process_info*)) {
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
                    events_file
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
    if (sync_states(proc, DONE)) {
        fprintf(stderr, "%d, Error on syncing\n", proc->id);
        return 4;
    }

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
    int args_res = get_process_count(argc, (const char **) argv, &process_count, &balances);

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
    if (start_processes(&parent, &child_pids, task)) {
        fprintf(stderr, "Failed to create child-processes\n");
        return 1;
    }

    // Wait for the processes to finish
    if (join_all(&parent, child_pids)) {
        fprintf(stderr, "Failed to create child-processes\n");
        return 2;
    }

    // Exit
    free(child_pids);
    return 0;
}

static int send_all(int fd, const void* buf, size_t left){
    const char* ptr = buf;
    ssize_t sent;

    while ((sent = write(fd, ptr, left)) >= 0) {
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

    while ((bytes_read = read(fd, ptr, left)) > 0) {
        left -= bytes_read; ptr += bytes_read;

        if (left == 0)
            return (int) bytes_read;
    }

    return -1;
}

int receive(void* self, local_id from, Message* msg){
    process_info* proc = self;

    if (receive_full(proc->channels[from].in_pipe, &(msg->s_header), sizeof(MessageHeader)) < 0) {
        fprintf(stderr, "Failed to receive header: %s\n", strerror(errno));
        return 1;
    }


    if (receive_full(proc->channels[from].in_pipe, msg->s_payload, msg->s_header.s_payload_len) < 0) {
        fprintf(stderr, "Failed to receive body: %s\n", strerror(errno));
        return 2;
    }

    return 0;
}

int receive_any(void* self, Message* msg){
    process_info* proc = self;

    while(1) {
        for (local_id i = 0; i < proc->p_count; i++) {
            if (i == proc->id) continue;

            if (!receive(proc, i, msg)) return 0;
        }
    }

}

//
// Created by taisia on 21.12.2021.
//

#include <stdio.h>
#include "request_queue.h"

int q_empty(request_queue* queue) {
    return !queue->size;
}

int8_t q_minimal(request_queue* queue) {
    if (q_empty(queue))
        return -1;
    else {
        timestamp_t min_time = INT16_MAX;
        local_id min_id = MAX_PROCESS_ID;
        int8_t val_idx;
//        printf("DEBUG queue: [");
        for (size_t i = 0; i < queue->size; i++) {
//            printf(" (%d, %d) ", queue->buffer[i].time, queue->buffer[i].id);
            if (min_time < queue->buffer[i].time ||
                (min_time == queue->buffer[i].time && min_id < queue->buffer[i].id)) continue;
            else {
                min_time = queue->buffer[i].time;
                min_id = queue->buffer[i].id;
                val_idx = (int8_t) i;
            }
        }
//        printf("]\nChose: %d\n", val_idx);
        return val_idx;
    }
}

local_id q_peek(request_queue* queue) {
    if (q_empty(queue))
        return -1;
    else
        return queue->buffer[q_minimal(queue)].id;
}

void q_push(request_queue* queue, local_id id, timestamp_t time) {
    queue->buffer[queue->size] = (lampert_pair) { id, time };
    queue->size++;
}

void q_delete(request_queue* queue, local_id id) {
    int8_t min_idx = q_minimal(queue);
    if (queue->buffer[min_idx].id == id)
        queue->buffer[min_idx] = queue->buffer[--queue->size];
}

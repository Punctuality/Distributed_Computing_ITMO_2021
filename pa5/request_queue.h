//
// Created by taisia on 21.12.2021.
//

#ifndef PA4_REQUEST_QUEUE_H
#define PA4_REQUEST_QUEUE_H

#include <stddef.h>
#include "ipc.h"

typedef struct {
    local_id id;
    timestamp_t time;
} lampert_pair;

typedef struct {
    size_t size;
    lampert_pair buffer[MAX_PROCESS_ID];
} request_queue;

int q_empty(request_queue* queue);

int8_t q_minimal(request_queue* queue);

local_id q_peek(request_queue* queue);

void q_push(request_queue* queue, local_id id, timestamp_t time);

void q_delete(request_queue* queue, local_id id);


#endif //PA4_REQUEST_QUEUE_H

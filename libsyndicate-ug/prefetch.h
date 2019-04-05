/*
   Copyright 2015 The Trustees of Princeton University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

/**
 * @file libsyndicate-ug/prefetch.h
 * @author Jude Nelson
 * @date 9 Mar 2016
 *
 * @brief Header file for inode.cpp related functions
 *
 * @see libsyndicate-ug/prefetch.cpp
 */

#ifndef _UG_PREFETCH_H_
#define _UG_PREFETCH_H_

#include <libsyndicate/libsyndicate.h>
#include <libsyndicate/gateway.h>
#include <libsyndicate/manifest.h>
#include <libsyndicate/util.h>
#include <libsyndicate/ms/ms-client.h>

#include "inode.h"
#include "block.h"

#define MAX_PREFETCH_LEN 4
#define MAX_FOOTPRINT_LEN   3

struct UG_read_buffer {
    char* buffer;
    off_t offset;
    int data_len;
    bool eof;
    size_t block_size;
    pthread_rwlock_t lock;
};

struct UG_read_prefetch_param {
    struct SG_gateway* gateway;
    char* fs_path;
    off_t offset;
    struct UG_file_handle* fh;
    pthread_rwlock_t lock;
};

struct UG_read_prefetch {
    struct UG_read_prefetch_param* param;
    struct UG_read_buffer* buffer;
    bool prefetch_thread_running;
    pthread_t prefetch_thread;
    pthread_rwlock_t lock;
};

struct UG_read_prefetch_queue_signal {
    uint32_t last_event;
};

typedef deque<off_t> UG_read_footprint_deque_t;
typedef queue<struct UG_read_prefetch*> UG_read_prefetch_queue_t;
typedef map<pthread_t, struct UG_read_prefetch_queue_signal*> UG_read_prefetch_queue_signal_map_t;

struct UG_read_prefetch_queue {
    UG_read_footprint_deque_t* footprint;
    bool perform_prefetch;
    UG_read_prefetch_queue_t* queue;
    UG_read_prefetch_queue_signal_map_t* signal_map;
    pthread_mutex_t signal_mutex;
    pthread_cond_t signal_cond;
    pthread_rwlock_t lock;
};

extern "C" {

off_t UG_read_prefetch_block_offset(off_t offset, size_t block_size);

// prefetch
struct UG_read_prefetch* UG_read_prefetch_new();
int UG_read_prefetch_init(struct UG_read_prefetch* prefetch, uint64_t block_size);
int UG_read_prefetch_free(struct UG_read_prefetch* prefetch);
int UG_read_prefetch_rlock(struct UG_read_prefetch* prefetch);
int UG_read_prefetch_wlock(struct UG_read_prefetch* prefetch);
int UG_read_prefetch_unlock(struct UG_read_prefetch* prefetch);

struct UG_read_buffer* UG_read_buffer_new();
int UG_read_buffer_init(struct UG_read_buffer* buffer, uint64_t block_size);
int UG_read_buffer_free(struct UG_read_buffer* buffer);
int UG_read_buffer_rlock(struct UG_read_buffer* buffer);
int UG_read_buffer_wlock(struct UG_read_buffer* buffer);
int UG_read_buffer_unlock(struct UG_read_buffer* buffer);
bool UG_read_buffer_check_offset_no_lock(struct UG_read_buffer* buffer, off_t offset);
int UG_read_buffer_copy_data(struct UG_read_buffer* buffer, char* buf, size_t buf_len, off_t offset, bool* eof);

struct UG_read_prefetch_param* UG_read_prefetch_param_new();
int UG_read_prefetch_param_init(struct UG_read_prefetch_param* param);
int UG_read_prefetch_param_free(struct UG_read_prefetch_param* param);
int UG_read_prefetch_param_rlock(struct UG_read_prefetch_param* param);
int UG_read_prefetch_param_wlock(struct UG_read_prefetch_param* param);
int UG_read_prefetch_param_unlock(struct UG_read_prefetch_param* param);

struct UG_read_prefetch_queue* UG_read_prefetch_queue_new();
int UG_read_prefetch_queue_init(struct UG_read_prefetch_queue* queue);
int UG_read_prefetch_queue_free(struct UG_read_prefetch_queue* queue);
int UG_read_prefetch_queue_rlock(struct UG_read_prefetch_queue* queue);
int UG_read_prefetch_queue_wlock(struct UG_read_prefetch_queue* queue);
int UG_read_prefetch_queue_unlock(struct UG_read_prefetch_queue* queue);
int UG_read_prefetch_queue_push(struct UG_read_prefetch_queue* queue, struct UG_read_prefetch* prefetch);
struct UG_read_prefetch* UG_read_prefetch_queue_pop(struct UG_read_prefetch_queue* queue);
struct UG_read_prefetch* UG_read_prefetch_queue_front(struct UG_read_prefetch_queue* queue);
int UG_read_prefetch_queue_clear(struct UG_read_prefetch_queue* queue);
int UG_read_prefetch_queue_len(struct UG_read_prefetch_queue* queue);
int UG_read_prefetch_queue_join_first(struct UG_read_prefetch_queue* queue);
int UG_read_prefetch_queue_retrieve_data(struct UG_read_prefetch_queue* queue, struct UG_read_buffer* buffer, off_t offset);
int UG_read_prefetch_queue_clear_stale(struct UG_read_prefetch_queue* queue, off_t offset);
int UG_read_prefetch_queue_add_footprint(struct UG_read_prefetch_queue* queue, off_t offset);
bool UG_read_prefetch_queue_check_prefetch_available(struct UG_read_prefetch_queue* queue);
int UG_read_prefetch_queue_set_prefetch_perform(struct UG_read_prefetch_queue* queue, bool perform);
int UG_read_prefetch_queue_determine_prefetch(struct UG_read_prefetch_queue* queue);

struct UG_read_prefetch_queue_signal* UG_read_prefetch_queue_signal_new();
int UG_read_prefetch_queue_signal_init(struct UG_read_prefetch_queue_signal* sig);
int UG_read_prefetch_queue_signal_free(struct UG_read_prefetch_queue_signal* sig);
int UG_read_prefetch_queue_wait(struct UG_read_prefetch_queue* queue, struct UG_read_prefetch* prefetch);
int UG_read_prefetch_queue_wakeup(struct UG_read_prefetch_queue* queue, struct UG_read_prefetch* prefetch, uint32_t event);

void UG_read_prefetch_download_connection_pool_event_handler(struct md_download_connection_pool* dlcpool, uint32_t event_type, void* event_data);
}
#endif

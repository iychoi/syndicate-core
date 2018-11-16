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
 * @file libsyndicate-ug/prefetch.cpp
 * @author Jude Nelson
 * @date 9 Mar 2016
 *
 * @brief User Gateway inode related functions
 *
 * @see libsyndicate-ug/prefetch.h
 */

#include "prefetch.h"
#include "block.h"

off_t UG_read_prefetch_block_offset(off_t offset, size_t block_size) {
    return (offset / block_size) * block_size;
}

struct UG_read_prefetch* UG_read_prefetch_new() {
    struct UG_read_prefetch* prefetch = SG_CALLOC(struct UG_read_prefetch, 1);
    return prefetch;
}

int UG_read_prefetch_init(struct UG_read_prefetch* prefetch, uint64_t block_size) {
    prefetch->param = UG_read_prefetch_param_new();
    UG_read_prefetch_param_init(prefetch->param);
    prefetch->buffer = UG_read_buffer_new();
    UG_read_buffer_init(prefetch->buffer, block_size);

    prefetch->prefetch_thread_running = false;

    pthread_rwlock_init(&prefetch->lock, NULL);
    return 0;
}

int UG_read_prefetch_free(struct UG_read_prefetch* prefetch) {
    // check if thread is joined
    UG_read_prefetch_wlock(prefetch);

    if(prefetch->prefetch_thread_running) {
        pthread_join(prefetch->prefetch_thread, NULL);
        prefetch->prefetch_thread_running = false;
    }

    UG_read_buffer_free(prefetch->buffer);
    SG_safe_free(prefetch->buffer);
    UG_read_prefetch_param_free(prefetch->param);
    SG_safe_free(prefetch->param);

    UG_read_prefetch_unlock(prefetch);
    pthread_rwlock_destroy(&prefetch->lock);

    memset(prefetch, 0, sizeof(struct UG_read_prefetch));
    return 0;
}

int UG_read_prefetch_rlock(struct UG_read_prefetch* prefetch) {
    SG_debug("RLOCK prefetch %p\n", prefetch);
    pthread_rwlock_rdlock(&prefetch->lock);
    return 0;
}

int UG_read_prefetch_wlock(struct UG_read_prefetch* prefetch) {
    SG_debug("WLOCK prefetch %p\n", prefetch);
    pthread_rwlock_wrlock(&prefetch->lock);
    return 0;
}

int UG_read_prefetch_unlock(struct UG_read_prefetch* prefetch) {
    SG_debug("UNLOCK prefetch %p\n", prefetch);
    pthread_rwlock_unlock(&prefetch->lock);
    return 0;
}

struct UG_read_buffer* UG_read_buffer_new() {
    struct UG_read_buffer* buffer = SG_CALLOC(struct UG_read_buffer, 1);
    return buffer;
}

int UG_read_buffer_init(struct UG_read_buffer* buffer, uint64_t block_size) {
    buffer->buffer = SG_CALLOC(char, block_size);
    buffer->offset = 0;
    buffer->data_len = 0;
    buffer->eof = false;
    buffer->block_size = block_size;
    pthread_rwlock_init(&buffer->lock, NULL);
    return 0;
}

int UG_read_buffer_free(struct UG_read_buffer* buffer) {
    UG_read_buffer_wlock(buffer);

    SG_safe_free(buffer->buffer);

    UG_read_buffer_unlock(buffer);
    pthread_rwlock_destroy(&buffer->lock);

    memset(buffer, 0, sizeof(struct UG_read_buffer));
    return 0;
}

int UG_read_buffer_rlock(struct UG_read_buffer* buffer) {
    SG_debug("RLOCK read_buffer %p\n", buffer);
    pthread_rwlock_rdlock(&buffer->lock);
    return 0;
}

int UG_read_buffer_wlock(struct UG_read_buffer* buffer) {
    SG_debug("WLOCK read_buffer %p\n", buffer);
    pthread_rwlock_wrlock(&buffer->lock);
    return 0;
}

int UG_read_buffer_unlock(struct UG_read_buffer* buffer) {
    SG_debug("UNLOCK read_buffer %p\n", buffer);
    pthread_rwlock_unlock(&buffer->lock);
    return 0;
}

bool UG_read_buffer_check_offset_no_lock(struct UG_read_buffer* buffer, off_t offset) {
    if(buffer->buffer == NULL || buffer->block_size <= 0) {
        return false;
    }

    if(buffer->offset <= offset && (off_t) (buffer->offset + buffer->block_size) >= offset) {
        return true;
    }
    return false;
}

int UG_read_buffer_copy_data(struct UG_read_buffer* buffer, char* buf, size_t buf_len, off_t offset, bool* eof) {
    UG_read_buffer_rlock(buffer);

    if(buffer->buffer == NULL || buffer->block_size <= 0) {
        UG_read_buffer_unlock(buffer);
        return -1;
    }

    if(buffer->offset <= offset && (off_t) (buffer->offset + buffer->block_size) >= offset) {
        *eof = buffer->eof;

        off_t boffset = offset - buffer->offset;
        int max_read = MIN(buffer->data_len - boffset, (int) buf_len); // available data len in the buffer
        if(max_read > 0) {
            memcpy(buf, buffer->buffer + boffset, max_read);
            UG_read_buffer_unlock(buffer);
            return max_read;
        } else if(max_read == 0) {
            UG_read_buffer_unlock(buffer);
            return 0;
        }
    }

    UG_read_buffer_unlock(buffer);
    return -1;
}

struct UG_read_prefetch_param* UG_read_prefetch_param_new() {
    struct UG_read_prefetch_param* param = SG_CALLOC(struct UG_read_prefetch_param, 1);
    return param;
}

int UG_read_prefetch_param_init(struct UG_read_prefetch_param* param) {
    param->gateway = NULL;
    param->fs_path = NULL;
    param->offset = 0;
    param->fh = NULL;
    pthread_rwlock_init(&param->lock, NULL);
    return 0;
}

int UG_read_prefetch_param_free(struct UG_read_prefetch_param* param) {
    UG_read_prefetch_param_wlock(param);
    SG_safe_free(param->fs_path);

    UG_read_prefetch_param_unlock(param);
    pthread_rwlock_destroy(&param->lock);

    memset(param, 0, sizeof(struct UG_read_prefetch_param));
    return 0;
}

int UG_read_prefetch_param_rlock(struct UG_read_prefetch_param* param) {
    SG_debug("RLOCK prefetch_param %p\n", param);
    pthread_rwlock_rdlock(&param->lock);
    return 0;
}

int UG_read_prefetch_param_wlock(struct UG_read_prefetch_param* param) {
    SG_debug("WLOCK prefetch_param %p\n", param);
    pthread_rwlock_wrlock(&param->lock);
    return 0;
}

int UG_read_prefetch_param_unlock(struct UG_read_prefetch_param* param) {
    SG_debug("UNLOCK prefetch_param %p\n", param);
    pthread_rwlock_unlock(&param->lock);
    return 0;
}

struct UG_read_prefetch_queue* UG_read_prefetch_queue_new() {
    struct UG_read_prefetch_queue* queue = SG_CALLOC(struct UG_read_prefetch_queue, 1);
    return queue;
}

int UG_read_prefetch_queue_init(struct UG_read_prefetch_queue* queue) {
    queue->queue = SG_safe_new(UG_read_prefetch_queue_t());
    queue->signal_map = SG_safe_new(UG_read_prefetch_queue_signal_map_t());

    queue->signal_mutex = PTHREAD_MUTEX_INITIALIZER;
    queue->signal_cond = PTHREAD_COND_INITIALIZER;

    pthread_rwlock_init(&queue->lock, NULL);
    return 0;
}

int UG_read_prefetch_queue_free(struct UG_read_prefetch_queue* queue) {
    UG_read_prefetch_queue_wlock(queue);

    if(queue->signal_map != NULL) {
        for( UG_read_prefetch_queue_signal_map_t::iterator itr = queue->signal_map->begin(); itr != queue->signal_map->end(); itr++ ) {
            struct UG_read_prefetch_queue_signal* sig = itr->second;
            UG_read_prefetch_queue_signal_free(sig);
            SG_safe_free(sig);
        }

        queue->signal_map->clear();
        SG_safe_delete(queue->signal_map);
    }

    if(queue->queue != NULL) {
        while(queue->queue->size() > 0) {
            struct UG_read_prefetch* prefetch = queue->queue->front();
            queue->queue->pop();
            UG_read_prefetch_free(prefetch);
            SG_safe_free(prefetch);
        }

        SG_safe_delete(queue->queue);
    }

    UG_read_prefetch_queue_unlock(queue);
    pthread_rwlock_destroy(&queue->lock);

    pthread_mutex_destroy(&queue->signal_mutex);
    pthread_cond_destroy(&queue->signal_cond);

    memset(queue, 0, sizeof(struct UG_read_prefetch_queue));
    return 0;
}

int UG_read_prefetch_queue_rlock(struct UG_read_prefetch_queue* queue) {
    SG_debug("RLOCK prefetch_queue %p\n", queue);
    pthread_rwlock_rdlock(&queue->lock);
    return 0;
}

int UG_read_prefetch_queue_wlock(struct UG_read_prefetch_queue* queue) {
    SG_debug("WLOCK prefetch_queue %p\n", queue);
    pthread_rwlock_wrlock(&queue->lock);
    return 0;
}

int UG_read_prefetch_queue_unlock(struct UG_read_prefetch_queue* queue) {
    SG_debug("UNLOCK prefetch_queue %p\n", queue);
    pthread_rwlock_unlock(&queue->lock);
    return 0;
}

int UG_read_prefetch_queue_push(struct UG_read_prefetch_queue* queue, struct UG_read_prefetch* prefetch) {
    int rc = 0;
    UG_read_prefetch_queue_wlock(queue);
    try {
        queue->queue->push(prefetch);
    }
    catch(bad_alloc& ba) {
        rc = -ENOMEM;
    }
    UG_read_prefetch_queue_unlock(queue);
    return rc;
}

struct UG_read_prefetch* UG_read_prefetch_queue_pop(struct UG_read_prefetch_queue* queue) {
    struct UG_read_prefetch* ret = NULL;
    UG_read_prefetch_queue_wlock(queue);
    if(queue->queue->size() > 0) {
        ret = queue->queue->front();
        queue->queue->pop();
    }
    UG_read_prefetch_queue_unlock(queue);
    return ret;
}

struct UG_read_prefetch* UG_read_prefetch_queue_front(struct UG_read_prefetch_queue* queue) {
    struct UG_read_prefetch* ret = NULL;
    UG_read_prefetch_queue_wlock(queue);
    if(queue->queue->size() > 0) {
        ret = queue->queue->front();
    }
    UG_read_prefetch_queue_unlock(queue);
    return ret;
}

int UG_read_prefetch_queue_clear(struct UG_read_prefetch_queue* queue) {
    UG_read_prefetch_queue_wlock(queue);
    while(queue->queue->size() > 0) {
        struct UG_read_prefetch* prefetch = queue->queue->front();
        bool running = false;

        queue->queue->pop();

        UG_read_prefetch_rlock(prefetch);
        if(prefetch->prefetch_thread_running) {
            running = true;
        }
        UG_read_prefetch_unlock(prefetch);

        if(running) {
            UG_read_prefetch_wlock(prefetch);
            if(prefetch->prefetch_thread_running) {
                SG_debug("joining the prefetch thread %p\n", &prefetch->prefetch_thread);
                pthread_join(prefetch->prefetch_thread, NULL);
                prefetch->prefetch_thread_running = false;
            }
            UG_read_prefetch_unlock(prefetch);
        }
        UG_read_prefetch_free(prefetch);
        SG_safe_free(prefetch);
    }

    if(queue->signal_map != NULL) {
        for( UG_read_prefetch_queue_signal_map_t::iterator itr = queue->signal_map->begin(); itr != queue->signal_map->end(); itr++ ) {
            struct UG_read_prefetch_queue_signal* sig = itr->second;
            UG_read_prefetch_queue_signal_free(sig);
            SG_safe_free(sig);
        }

        queue->signal_map->clear();
    }
    UG_read_prefetch_queue_unlock(queue);
    return 0;
}

int UG_read_prefetch_queue_len(struct UG_read_prefetch_queue* queue) {
    int size = 0;
    UG_read_prefetch_queue_rlock(queue);
    size = queue->queue->size();
    UG_read_prefetch_queue_unlock(queue);
    return size;
}

int UG_read_prefetch_queue_join_first(struct UG_read_prefetch_queue* queue) {
    int rc = -1;
    UG_read_prefetch_queue_rlock(queue);
    if(queue->queue->size() > 0) {
        struct UG_read_prefetch* prefetch = queue->queue->front();
        bool running = false;
        UG_read_prefetch_rlock(prefetch);
        if(prefetch->prefetch_thread_running) {
            running = true;
        }
        UG_read_prefetch_unlock(prefetch);

        if(running) {
            UG_read_prefetch_wlock(prefetch);
            if(prefetch->prefetch_thread_running) {
                SG_debug("joining the prefetch thread %p\n", &prefetch->prefetch_thread);
                pthread_join(prefetch->prefetch_thread, NULL);
                prefetch->prefetch_thread_running = false;
            }
            UG_read_prefetch_unlock(prefetch);
        }
        rc = 0;
    }
    UG_read_prefetch_queue_unlock(queue);
    return rc;
}

int UG_read_prefetch_queue_retrieve_data(struct UG_read_prefetch_queue* queue, struct UG_read_buffer* buffer, off_t offset) {
    int rc = -1;
    UG_read_prefetch_queue_rlock(queue);
    if(queue->queue->size() > 0) {
        struct UG_read_prefetch* prefetch = queue->queue->front();
        UG_read_prefetch_rlock(prefetch);
        UG_read_buffer_wlock(prefetch->buffer);
        UG_read_buffer_wlock(buffer);

        if(UG_read_buffer_check_offset_no_lock(prefetch->buffer, offset)) {
            memcpy(buffer->buffer, prefetch->buffer->buffer, prefetch->buffer->data_len);
            buffer->offset = prefetch->buffer->offset;
            buffer->data_len = prefetch->buffer->data_len;
            buffer->eof = prefetch->buffer->eof;
            buffer->block_size = prefetch->buffer->block_size;
            queue->queue->pop();
            rc = 0;

            UG_read_buffer_unlock(buffer);
            UG_read_buffer_unlock(prefetch->buffer);

            UG_read_prefetch_unlock(prefetch);
            UG_read_prefetch_free(prefetch);
            SG_safe_free(prefetch);
        } else {
            UG_read_buffer_unlock(buffer);
            UG_read_buffer_unlock(prefetch->buffer);

            UG_read_prefetch_unlock(prefetch);
        }
    }
    UG_read_prefetch_queue_unlock(queue);
    return rc;
}

int UG_read_prefetch_queue_clear_stale(struct UG_read_prefetch_queue* queue, off_t offset) {
    UG_read_prefetch_queue_wlock(queue);
    while(queue->queue->size() > 0) {
        struct UG_read_prefetch* prefetch = queue->queue->front();

        UG_read_prefetch_wlock(prefetch);
        UG_read_buffer_rlock(prefetch->buffer);

        if(!UG_read_buffer_check_offset_no_lock(prefetch->buffer, offset)) {
            // stale
            UG_read_buffer_unlock(prefetch->buffer);

            if(prefetch->prefetch_thread_running) {
                SG_debug("joining the prefetch thread %p\n", &prefetch->prefetch_thread);
                pthread_join(prefetch->prefetch_thread, NULL);
                prefetch->prefetch_thread_running = false;
            }

            UG_read_prefetch_unlock(prefetch);

            queue->queue->pop();
            UG_read_prefetch_free(prefetch);
            SG_safe_free(prefetch);
        } else {
            UG_read_buffer_unlock(prefetch->buffer);
            UG_read_prefetch_unlock(prefetch);
            break;
        }
    }
    UG_read_prefetch_queue_unlock(queue);
    return 0;
}

struct UG_read_prefetch_queue_signal* UG_read_prefetch_queue_signal_new() {
    struct UG_read_prefetch_queue_signal* sig = SG_CALLOC(struct UG_read_prefetch_queue_signal, 1);
    return sig;
}

int UG_read_prefetch_queue_signal_init(struct UG_read_prefetch_queue_signal* sig) {
    sig->last_event = 0;
    return 0;
}

int UG_read_prefetch_queue_signal_free(struct UG_read_prefetch_queue_signal* sig) {
    memset(sig, 0, sizeof(struct UG_read_prefetch_queue_signal));
    return 0;
}

int UG_read_prefetch_queue_wait(struct UG_read_prefetch_queue* queue, struct UG_read_prefetch* prefetch) {
    bool has_signal = false;
    UG_read_prefetch_queue_signal_map_t::iterator itr;

    pthread_mutex_lock(&queue->signal_mutex);
    // check if there is a signal received
    itr = queue->signal_map->find(prefetch->prefetch_thread);
    if(itr != queue->signal_map->end()) {
       // found!
       has_signal = true;
    }

    if(has_signal) {
        SG_debug("Found a pending request-done-signal %p\n", prefetch);
    }

    while(!has_signal) {
        SG_debug("Wait request-done-signal %p\n", prefetch);
        pthread_cond_wait(&queue->signal_cond, &queue->signal_mutex);
        SG_debug("Received a request-done-signal %p\n", prefetch);

        itr = queue->signal_map->find(prefetch->prefetch_thread);
        if(itr != queue->signal_map->end()) {
           // found!
           has_signal = true;
           SG_debug("Received a request-done-signal - quiting %p\n", prefetch);
        }
    }
    pthread_mutex_unlock(&queue->signal_mutex);
    return 0;
}

int UG_read_prefetch_queue_wakeup(struct UG_read_prefetch_queue* queue, struct UG_read_prefetch* prefetch, uint32_t event) {
    struct UG_read_prefetch_queue_signal* sig = NULL;
    UG_read_prefetch_queue_signal_map_t::iterator itr;

    pthread_mutex_lock(&queue->signal_mutex);
    // check if there is a signal received
    itr = queue->signal_map->find(prefetch->prefetch_thread);
    if( itr != queue->signal_map->end() ) {
       // found!
       sig = itr->second;
       // update
       sig->last_event = event;
    } else {
       // add
       sig = UG_read_prefetch_queue_signal_new();
       UG_read_prefetch_queue_signal_init(sig);
       sig->last_event = event;
       (*queue->signal_map)[prefetch->prefetch_thread] = sig;
       // wakeup
       SG_debug("Broadcast a request-done-signal %p, %d\n", queue, event);
       pthread_cond_broadcast(&queue->signal_cond);
    }
    pthread_mutex_unlock(&queue->signal_mutex);
    return 0;
}

int UG_read_prefetch_queue_wakeup2(struct UG_read_prefetch_queue* queue, pthread_t prefetch_thread, uint32_t event) {
    struct UG_read_prefetch_queue_signal* sig = NULL;
    UG_read_prefetch_queue_signal_map_t::iterator itr;

    pthread_mutex_lock(&queue->signal_mutex);
    // check if there is a signal received
    itr = queue->signal_map->find(prefetch_thread);
    if( itr != queue->signal_map->end() ) {
       // found!
       sig = itr->second;
       // update
       sig->last_event = event;
    } else {
       // add
       sig = UG_read_prefetch_queue_signal_new();
       UG_read_prefetch_queue_signal_init(sig);
       sig->last_event = event;
       (*queue->signal_map)[prefetch_thread] = sig;
       // wakeup
       SG_debug("Broadcast a request-done-signal %p, %d\n", queue, event);
       pthread_cond_broadcast(&queue->signal_cond);
    }
    pthread_mutex_unlock(&queue->signal_mutex);
    return 0;
}

void UG_read_prefetch_download_connection_pool_event_handler(struct md_download_connection_pool* dlcpool, uint32_t event_type, void* event_data) {
    void* userdata = md_download_connection_pool_get_user_data(dlcpool);
    struct UG_file_handle* fh = (struct UG_file_handle*)userdata;
    struct UG_read_prefetch_queue* prefetch_queue;
    pthread_t mythread = pthread_self();

    if(fh == NULL) {
        SG_error("file handle is not set for %p\n", dlcpool);
        return;
    }

    prefetch_queue = fh->prefetch_queue;
    switch(event_type) {
        case MD_DOWNLOAD_CONNECTION_POOL_EVENT_HTTP_REQUEST:
            UG_read_prefetch_queue_wakeup2(prefetch_queue, mythread, MD_DOWNLOAD_CONNECTION_POOL_EVENT_HTTP_REQUEST);
            break;
        case MD_DOWNLOAD_CONNECTION_POOL_EVENT_FINISH_USE_CONNECTION:
            UG_read_prefetch_queue_wakeup2(prefetch_queue, mythread, MD_DOWNLOAD_CONNECTION_POOL_EVENT_FINISH_USE_CONNECTION);
            break;
    }
}

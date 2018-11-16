/*
   Copyright 2014 The Trustees of Princeton University

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
 * @file libsyndicate/download.cpp
 * @author Jude Nelson
 * @date 9 Mar 2016
 *
 * @brief Functions to support download capabilities
 *
 * @see libsyndicate/download.h
 */

#include "libsyndicate/download.h"

/**
 * @brief Download contexts set
 */
struct md_download_set {

   md_pending_set_t* waiting;           ///< pointers to download contexts for which we are waiting

   sem_t sem;                           ///< block on this until at least one of waiting has been finalized
};

/**
 * @brief Download context
 */
struct md_download_context {

   struct md_bound_response_buffer brb; ///< A bounded response buffer

   CURL* curl;                    ///< Ptr to a CURL object

   int curl_rc;                   ///< Stores CURL error code
   int http_status;               ///< Stores HTTP status
   int transfer_errno;            ///< Stores CURL-reported system errno, if an error occurred
   volatile bool cancelled;       ///< If true, this was cancelled
   char* effective_url;           ///< Stores final URL that resolved to data

   volatile bool initialized;     ///< If true, then this download context has been initialized
   volatile bool pending;         ///< If true, then this download context is in the process of being started
   volatile bool cancelling;      ///< If true, then this download context is in the process of being cancelled
   volatile bool running;         ///< If true, then this download is enqueued on the downloader
   volatile bool finalized;       ///< If true, then this download has finished
   int ref_count;                 ///< Number of threads referencing this download

   pthread_mutex_t finalize_lock; ///< Lock to serialize operations that change the above flags (primarily related to finalization)

   struct md_download_set* dlset; ///< Parent group containing this context

   sem_t sem;                     ///< Client holds this to be woken up when the download finishes

   void* cls;                     ///< Associated download state
};


/**
 * @brief Download worker
 *
 * Container for download information
 */
struct md_downloader {

   char* name;                          ///< Downloader name
   pthread_t thread;                    ///< CURL thread for downloading

   md_downloading_map_t* downloading;   ///< Currently-running downloads
   pthread_rwlock_t downloading_lock;   ///< Guards downloading and curlm

   md_pending_set_t* pending;           ///< To be inserted into the downloading map
   pthread_rwlock_t pending_lock;       ///< Guards pending
   volatile bool has_pending;           ///< Flag that download is pending

   md_pending_set_t* cancelling;        ///< To be removed from the downloading map
   pthread_rwlock_t cancelling_lock;    ///< Guards cancelling_lock
   volatile bool has_cancelling;        ///< Flag that a download is cancelling

   CURLM* curlm;                        ///< Multi-download

   bool running;                        ///< If true, then this downloader is running
   bool inited;                         ///< If true, then this downloader is fully initialized
};


/**
 * @brief Download loop structure
 *
 * Container for downloads
 */
struct md_download_loop {

   struct md_downloader* dl;               ///< Downloader

   struct md_download_context** downloads; ///< Downloads
   int num_downloads;                      ///< Number of downloads

   struct md_download_set dlset;           ///< A download set with download contexts for which we are waiting

   bool started;                           ///< Flag whether the download loop is in progress
};

static void* md_downloader_main( void* arg );
int md_downloader_finalize_download_context( struct md_download_context* dlctx, int curl_rc );

// download context sets (like an FDSET)
int md_download_set_init( struct md_download_set* dlset );
int md_download_set_free( struct md_download_set* dlset );
int md_download_set_add( struct md_download_set* dlset, struct md_download_context* dlctx );
int md_download_set_clear( struct md_download_set* dlset, struct md_download_context* dlctx );    // don't use inside a e.g. for() loop where you're iterating over a download set
int md_download_set_wakeup( struct md_download_set* dlset );
size_t md_download_set_size( struct md_download_set* dlset );
bool md_download_set_contains( struct md_download_set* dlset, struct md_download_context* dlctx );

// iterating through waiting
md_download_set_iterator md_download_set_begin( struct md_download_set* dlset );
md_download_set_iterator md_download_set_end( struct md_download_set* dlset );
struct md_download_context* md_download_set_iterator_get_context( const md_download_set_iterator& itr );


/// Read lock downloading contexts
int md_downloader_downloading_rlock( struct md_downloader* dl ) {
   return pthread_rwlock_rdlock( &dl->downloading_lock );
}

/// Write lock downloading contexts
int md_downloader_downloading_wlock( struct md_downloader* dl ) {
   return pthread_rwlock_wrlock( &dl->downloading_lock );
}

/// Unlock downloading contexts
int md_downloader_downloading_unlock( struct md_downloader* dl ) {
   return pthread_rwlock_unlock( &dl->downloading_lock );
}

/// Read lock the pending contexts
int md_downloader_pending_rlock( struct md_downloader* dl ) {
   return pthread_rwlock_rdlock( &dl->pending_lock );
}

/// Write lock the pending contexts
int md_downloader_pending_wlock( struct md_downloader* dl ) {
   return pthread_rwlock_wrlock( &dl->pending_lock );
}

/// Unlock the pending contexts
int md_downloader_pending_unlock( struct md_downloader* dl ) {
   return pthread_rwlock_unlock( &dl->pending_lock );
}

/// Read lock the pending contexts
int md_downloader_cancelling_rlock( struct md_downloader* dl ) {
   return pthread_rwlock_rdlock( &dl->cancelling_lock );
}

/// Write lock the cancelling context
int md_downloader_cancelling_wlock( struct md_downloader* dl ) {
   return pthread_rwlock_wrlock( &dl->cancelling_lock );
}

/// Unlock the cancelling context
int md_downloader_cancelling_unlock( struct md_downloader* dl ) {
   return pthread_rwlock_unlock( &dl->cancelling_lock );
}


/// Allocate a downloader
struct md_downloader* md_downloader_new() {
   return SG_CALLOC( struct md_downloader, 1 );
}

/**
 * @brief Set up a downloader
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 */
int md_downloader_init( struct md_downloader* dl, char const* name ) {

   int rc = 0;

   memset( dl, 0, sizeof(struct md_downloader) );

   rc = pthread_rwlock_init( &dl->downloading_lock, NULL );
   if( rc != 0 ) {

      return -rc;
   }

   rc = pthread_rwlock_init( &dl->pending_lock, NULL );
   if( rc != 0 ) {

      pthread_rwlock_destroy( &dl->downloading_lock );
      return -rc;
   }

   rc = pthread_rwlock_init( &dl->cancelling_lock, NULL );
   if( rc != 0 ) {

      pthread_rwlock_destroy( &dl->downloading_lock );
      pthread_rwlock_destroy( &dl->pending_lock );
      return -rc;
   }

   dl->curlm = curl_multi_init();

   dl->name = SG_strdup_or_null( name );
   dl->downloading = SG_safe_new( md_downloading_map_t() );
   dl->pending = SG_safe_new( md_pending_set_t() );
   dl->cancelling = SG_safe_new( md_pending_set_t() );

   if( dl->name == NULL || dl->downloading == NULL || dl->pending == NULL || dl->cancelling == NULL || dl->curlm == NULL ) {

      SG_safe_free( dl->name );
      SG_safe_free( dl->downloading );
      SG_safe_free( dl->pending );
      SG_safe_free( dl->cancelling );

      if( dl->curlm != NULL ) {
         curl_multi_cleanup( dl->curlm );
      }

      pthread_rwlock_destroy( &dl->downloading_lock );
      pthread_rwlock_destroy( &dl->pending_lock );
      pthread_rwlock_destroy( &dl->cancelling_lock );

      return -ENOMEM;
   }

   dl->inited = true;
   return 0;
}

/**
 * @brief Start up a downloader
 * @retval 0 Success
 * @retval -1 if we failed to start the thread, or if we're already running
 */
int md_downloader_start( struct md_downloader* dl ) {

   int rc = 0;
   if( !dl->running ) {

      dl->running = true;

      rc = md_start_thread( &dl->thread, md_downloader_main, dl, false );
      if( rc < 0 ) {

         SG_error("%s: md_start_thread rc = %d\n", dl->name, rc);
         dl->running = false;
         return -1;
      }
   }

   return 0;
}

/**
 * @brief Stop a downloader
 * @retval 0 Success
 * @retval <0 Failed to join with the downloader thread
 */
int md_downloader_stop( struct md_downloader* dl ) {

   if( dl->running ) {
      dl->running = false;

      int rc = pthread_join( dl->thread, NULL );
      if( rc != 0 ) {
         SG_error("%s: pthread_join rc = %d\n", dl->name, rc );
         return rc;
      }
   }

   return 0;
}

/**
 * @brief Signal every element of a pending_set by incrementing its semaphore
 * @note Always succeeds
 */
static int md_signal_pending_set( md_pending_set_t* ps ) {

   // signal each waiting thread
   for( md_pending_set_t::iterator itr = ps->begin(); itr != ps->end(); itr++ ) {

      struct md_download_context* dlctx = *itr;

      if( dlctx != NULL ) {
         SG_debug("Wakeup %p\n", dlctx);
         sem_post( &dlctx->sem );
      }
   }

   return 0;
}


/**
 * @brief Shut down a downloader
 *
 * Stops all CURL transfers abruptly.
 * @note The downloader must be stopped first.
 * @retval 0 Success
 * @retval -EINVAL The downloader was running, or if the downloader was never initialized
 */
int md_downloader_shutdown( struct md_downloader* dl ) {

   if( dl->running ) {
      // not stopped yet
      return -EINVAL;
   }

   if( !dl->inited ) {
      // not initialized
      return -EINVAL;
   }

   // destroy downloading
   md_downloader_downloading_wlock( dl );

   dl->inited = false;

   if( dl->downloading != NULL ) {

      // remove each running download and signal the waiting threads
      for( md_downloading_map_t::iterator itr = dl->downloading->begin(); itr != dl->downloading->end(); itr++ ) {

         struct md_download_context* dlctx = itr->second;

         curl_multi_remove_handle( dl->curlm, dlctx->curl );

         sem_post( &dlctx->sem );
      }

      dl->downloading->clear();

      delete dl->downloading;
      dl->downloading = NULL;
   }

   if( dl->curlm != NULL ) {
      curl_multi_cleanup( dl->curlm );
      dl->curlm = NULL;
   }

   md_downloader_downloading_unlock( dl );

   // destroy pending
   md_downloader_pending_wlock( dl );

   if( dl->pending != NULL ) {

      // signal each waiting thread
      md_signal_pending_set( dl->pending );

      dl->pending->clear();
      delete dl->pending;
      dl->pending = NULL;
   }

   md_downloader_pending_unlock( dl );

   md_downloader_cancelling_wlock( dl );

   // destroy cancelling
   if( dl->cancelling != NULL ) {

      // signal each waiting thread
      md_signal_pending_set( dl->cancelling );

      dl->cancelling->clear();
      delete dl->cancelling;
      dl->cancelling = NULL;
   }

   md_downloader_cancelling_unlock( dl );

   // misc
   if( dl->name ) {
      free( dl->name );
      dl->name = NULL;
   }

   pthread_rwlock_destroy( &dl->downloading_lock );
   pthread_rwlock_destroy( &dl->pending_lock );
   pthread_rwlock_destroy( &dl->cancelling_lock );

   memset( dl, 0, sizeof(struct md_downloader) );

   return 0;
}

/**
 * @brief Get run state of downloader
 * @retval True Running
 * @retval False Not running
 */
bool md_downloader_is_running( struct md_downloader* dl ) {
   return dl->running;
}


/**
 * @brief Insert a pending context.
 *
 * This increments the download context's reference count.
 * @retval 0 Success, and mark it as pending and unsafe to free Success
 * @retval -EPERM The downloader is not running
 * @retval -EINVAL The download context is finalized, already pending, or already cancelling
 * @retval -ENOMEM Out of Memory
 */
int md_downloader_insert_pending( struct md_downloader* dl, struct md_download_context* dlctx ) {

   int rc = 0;

   md_downloader_pending_wlock( dl );

   if( !dl->running ) {
      md_downloader_pending_unlock( dl );
      return -EPERM;
   }

   pthread_mutex_lock( &dlctx->finalize_lock );

   if( dlctx->finalized ) {
      md_downloader_pending_unlock( dl );

      pthread_mutex_unlock( &dlctx->finalize_lock );
      return -EINVAL;
   }

   if( dlctx->pending || dlctx->cancelling ) {
      md_downloader_pending_unlock( dl );

      pthread_mutex_unlock( &dlctx->finalize_lock );
      return -EINVAL;
   }

   dlctx->pending = true;

   try {
      dl->pending->insert( dlctx );
   }
   catch( bad_alloc& ba ) {

      dlctx->pending = false;
      rc = -ENOMEM;
   }

   // reference--the downloader has a ref to it.
   dlctx->ref_count++;
   SG_debug("download %p ref %d\n", dlctx, dlctx->ref_count );

   pthread_mutex_unlock( &dlctx->finalize_lock );

   md_downloader_pending_unlock( dl );

   if( rc == 0 ) {
      dl->has_pending = true;

      SG_debug("Start download context %p\n", dlctx );
   }

   return rc;
}


/**
 * @brief Insert a context to cancel
 * @retval 0 Success, and mark the download context as cancelling
 * @retval -EPERM The downloader was not running
 * @retval -EINPROGRESS The download context is already cancelling
 * @retval -ENOMEM Out of Memory
 */
int md_downloader_insert_cancelling( struct md_downloader* dl, struct md_download_context* dlctx ) {

   int rc = 0;
   SG_debug("Cancel download context %p\n", dlctx );

   md_downloader_cancelling_wlock( dl );

   if( !dl->running ) {
      md_downloader_cancelling_unlock( dl );
      return -EPERM;
   }

   pthread_mutex_lock( &dlctx->finalize_lock );

   if( dlctx->finalized ) {
      md_downloader_cancelling_unlock( dl );
      pthread_mutex_unlock( &dlctx->finalize_lock );

      SG_warn("Download context %p is already finalized\n", dlctx );
      return 0;
   }

   if( dlctx->cancelling ) {
      md_downloader_cancelling_unlock( dl );
      pthread_mutex_unlock( &dlctx->finalize_lock );

      SG_warn("Download context %p is already cancelling\n", dlctx );
      return -EINPROGRESS;
   }

   dlctx->cancelling = true;

   try {
      if( !dlctx->pending ) {
         dl->cancelling->insert( dlctx );
      }
   }
   catch( bad_alloc& ba ) {
      rc = -ENOMEM;
      dlctx->cancelling = false;
   }

   // reference this--the downloader has a ref to it
   dlctx->ref_count++;
   SG_debug("download %p ref %d\n", dlctx, dlctx->ref_count );

   pthread_mutex_unlock( &dlctx->finalize_lock );

   dl->has_cancelling = true;

   md_downloader_cancelling_unlock( dl );

   return rc;
}

/**
 * @brief Add all pending to downloading
 * @note The downloader must be write-locked for downloading
 * @retval 0 Success
 * @retval -EPERM Failed to insert the curl handle
 * @retval -ENOMEM Out of Memory
 */
int md_downloader_start_all_pending( struct md_downloader* dl ) {

   int rc = 0;

   if( dl->has_pending ) {

      md_downloader_pending_wlock( dl );

      for( md_pending_set_t::iterator itr = dl->pending->begin(); itr != dl->pending->end(); itr++ ) {

         struct md_download_context* dlctx = *itr;

         if( dlctx == NULL ) {
            continue;
         }

         pthread_mutex_lock( &dlctx->finalize_lock );

         if( md_download_context_finalized( dlctx ) ) {

            pthread_mutex_unlock( &dlctx->finalize_lock );
            continue;
         }

         if( dlctx->cancelling ) {

            // got cancelled quickly after insertion
            dlctx->cancelled = true;
            dlctx->cancelling = false;

            pthread_mutex_unlock( &dlctx->finalize_lock );

            rc = md_downloader_finalize_download_context( dlctx, -EAGAIN );
            if( rc > 0 ) {

               // this was the last reference to the download context
               CURL* curl = NULL;
               md_download_context_free( dlctx, &curl );
               curl_easy_cleanup( curl );
               SG_safe_free( dlctx );       // allowed, since dlctx can only be heap-allocated

               rc = 0;
            }

            continue;
         }

         // add the handle
         rc = curl_multi_add_handle( dl->curlm, dlctx->curl );
         if( rc != 0 ) {

            SG_error("curl_multi_add_handle( %p ) rc = %d\n", dlctx, rc );

            rc = -EPERM;
            pthread_mutex_unlock( &dlctx->finalize_lock );
            break;
         }

         dlctx->running = true;
         dlctx->pending = false;

         pthread_mutex_unlock( &dlctx->finalize_lock );

         try {
            (*dl->downloading)[ dlctx->curl ] = dlctx;
         }
         catch( bad_alloc& ba ) {
            rc = -ENOMEM;
            break;
         }
      }

      dl->pending->clear();

      dl->has_pending = false;

      md_downloader_pending_unlock( dl );
   }

   return rc;
}


/**
 * @brief Remove all cancelling downloads from downloading
 *
 * Unreferences downloads and frees them if needed.
 * @note The downloader must be write-locked for downloading
 * @retval 0 Success
 * @retval -EPERM Failed to remove the curl handle
 * @retval -ENOMEM Out of Memory
 */
int md_downloader_end_all_cancelling( struct md_downloader* dl ) {

   int rc = 0;

   if( dl->has_cancelling ) {

      md_downloader_cancelling_wlock( dl );

      for( md_pending_set_t::iterator itr = dl->cancelling->begin(); itr != dl->cancelling->end(); itr++ ) {

         struct md_download_context* dlctx = *itr;

         if( dlctx == NULL ) {
            continue;
         }

         pthread_mutex_lock( &dlctx->finalize_lock );

         rc = curl_multi_remove_handle( dl->curlm, dlctx->curl );
         if( rc != 0 ) {

            SG_error("curl_multi_remove_handle( %p ) rc = %d\n", dlctx, rc );

            rc = -EPERM;
            pthread_mutex_unlock( &dlctx->finalize_lock );
            continue;
         }

         // NOTE: this is the unref that the client would have called, had they not cancelled
         dlctx->ref_count--;
         SG_debug("download %p ref %d\n", dlctx, dlctx->ref_count );
         dl->downloading->erase( dlctx->curl );

         // update state
         dlctx->cancelled = true;
         dlctx->cancelling = false;

         // struct md_download_set* dlset = dlctx->dlset;

         pthread_mutex_unlock( &dlctx->finalize_lock );

         // finalize, with -EAGAIN
         rc = md_downloader_finalize_download_context( dlctx, -EAGAIN );
         if( rc > 0 ) {

            // this was the last reference to the download context
            CURL* curl = NULL;
            md_download_context_free( dlctx, &curl );
            curl_easy_cleanup( curl );
            SG_safe_free( dlctx );      // allowed, since dlctx can only be heap-allocated

            rc = 0;
         }
         /*
         // wake up the set waiting on this dlctx
         if( dlset != NULL ) {

            rc = md_download_set_wakeup( dlset );
            if( rc != 0 ) {

               SG_error("md_download_set_wakeup( %p ) rc = %d\n", dlset, rc );
               rc = 0;
               continue;
            }
         }
         */
      }

      dl->cancelling->clear();

      dl->has_cancelling = false;

      md_downloader_cancelling_unlock( dl );
   }

   return 0;
}


/**
 * @brief Download data to a response buffer
 * @retval Size Success
 * @retval 0 Out of Memory
 */
size_t md_get_callback_response_buffer( void* stream, size_t size, size_t count, void* user_data ) {

   md_response_buffer_t* rb = (md_response_buffer_t*)user_data;

   size_t realsize = size * count;
   char* buf = SG_CALLOC( char, realsize );

   if( buf == NULL ) {
      // OOM
      return 0;
   }

   memcpy( buf, stream, realsize );

   try {
      rb->push_back( md_buffer_segment_t( buf, realsize ) );
   }
   catch( bad_alloc& ba ) {

      SG_safe_free( buf );
      return 0;
   }

   return realsize;
}

/**
 * @brief Download to a bound response buffer
 * @retval Size Success
 * @retval 0 Out of Memory
 */
size_t md_get_callback_bound_response_buffer( void* stream, size_t size, size_t count, void* user_data ) {

   struct md_bound_response_buffer* brb = (struct md_bound_response_buffer*)user_data;

   // SG_debug("size = %zu, count = %zu, max_size = %ld, size = %ld\n", size, count, brb->max_size, brb->size );

   if( brb->size >= brb->max_size ) {
      return 0;
   }

   off_t realsize = size * count;
   if( brb->max_size >= 0 && (off_t)(brb->size + realsize) > brb->max_size ) {

      realsize = brb->max_size - brb->size;
      if( realsize < 0 ) {
         return 0;
      }
   }

   char* buf = SG_CALLOC( char, realsize );
   if( buf == NULL ) {
      return 0;
   }

   memcpy( buf, stream, realsize );

   try {
      brb->rb->push_back( md_buffer_segment_t( buf, realsize ) );
   }
   catch( bad_alloc& ba ) {

      SG_safe_free( buf );
      return 0;
   }

   brb->size += realsize;

   return realsize;
}


/**
 * @brief Alloc a download context
 */
struct md_download_context* md_download_context_new() {
   return SG_CALLOC( struct md_download_context, 1 );
}


/**
 * @brief Initialize a download context.  Takes a CURL handle from the client.
 *
   The only things it sets in the CURL handle are:
   CURLOPT_WRITEDATA
   CURLOPT_WRITEFUNCTION
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 * @retval -errno on failure to initialize a pthread lock
 */
int md_download_context_init( struct md_download_context* dlctx, CURL* curl, off_t max_len, void* cls ) {

   SG_debug("Initialize download context %p\n", dlctx );

   int rc = 0;
   memset( dlctx, 0, sizeof(struct md_download_context) );

   rc = pthread_mutex_init( &dlctx->finalize_lock, NULL );
   if( rc != 0 ) {
      return -rc;
   }

   rc = md_bound_response_buffer_init( &dlctx->brb, max_len );
   if( rc != 0 ) {

      pthread_mutex_destroy( &dlctx->finalize_lock );
      return rc;
   }

   dlctx->curl = curl;
   dlctx->cls = cls;

   sem_init( &dlctx->sem, 0, 0 );

   curl_easy_setopt( dlctx->curl, CURLOPT_WRITEDATA, (void*)&dlctx->brb );
   curl_easy_setopt( dlctx->curl, CURLOPT_WRITEFUNCTION, md_get_callback_bound_response_buffer );

   dlctx->dlset = NULL;
   dlctx->initialized = true;

   return 0;
}


/**
 * @brief Reset a download context.
 * @note Don't call this until it's finalized
 * @retval 0 Success
 * @retval -EAGAIN The download context is not finalized
 */
int md_download_context_reset( struct md_download_context* dlctx, CURL** old_curl ) {

   SG_debug("Reset download context %p\n", dlctx );

   pthread_mutex_lock( &dlctx->finalize_lock );

   if( !md_download_context_finalized( dlctx ) ) {
      SG_error("Download %p not yet finalized\n", dlctx );

      pthread_mutex_unlock( &dlctx->finalize_lock );
      return -EAGAIN;
   }

   md_response_buffer_free( dlctx->brb.rb );
   dlctx->brb.size = 0;

   curl_easy_setopt( dlctx->curl, CURLOPT_WRITEDATA, (void*)&dlctx->brb );
   curl_easy_setopt( dlctx->curl, CURLOPT_WRITEFUNCTION, md_get_callback_bound_response_buffer );

   dlctx->curl_rc = 0;
   dlctx->http_status = 0;
   dlctx->transfer_errno = 0;
   dlctx->cancelled = false;
   dlctx->finalized = false;
   dlctx->pending = false;
   dlctx->cancelling = false;
   dlctx->running = false;
   dlctx->cls = NULL;
   dlctx->ref_count = 0;
   SG_debug("download %p ref-set %d\n", dlctx, dlctx->ref_count );

   if( dlctx->effective_url != NULL ) {

      SG_safe_free( dlctx->effective_url );
   }

   if( old_curl != NULL ) {
      *old_curl = dlctx->curl;
   }

   pthread_mutex_unlock( &dlctx->finalize_lock );

   return 0;
}


/**
 * @brief Free a download context.
 *
 * If this download context was finalized, then it's guaranteed to be freed
 * @note Does *not* check reference count
 * @note Always succeeds
 * @return 0
 */
int md_download_context_free2( struct md_download_context* dlctx, CURL** curl, char const* file, int lineno ) {

   pthread_mutex_lock( &dlctx->finalize_lock );

   SG_debug("Free download context %p, from %s:%d (refcount %d)\n", dlctx, file, lineno, dlctx->ref_count );
   if( dlctx->ref_count > 0 ) {

      SG_error("BUG: download context %p has %d references\n", dlctx, dlctx->ref_count );
      exit(1);
   }


   md_bound_response_buffer_free( &dlctx->brb );

   if( curl != NULL ) {
      *curl = dlctx->curl;
   }

   if( dlctx->effective_url != NULL ) {

      SG_safe_free( dlctx->effective_url );
   }

   dlctx->curl = NULL;

   sem_destroy( &dlctx->sem );

   pthread_mutex_unlock( &dlctx->finalize_lock );
   pthread_mutex_destroy( &dlctx->finalize_lock );

   memset( dlctx, 0, sizeof(struct md_download_context));

   return 0;
}


/**
 * @brief Reference this download context
 * @note Always succeeds
 * @return 0
 */
int md_download_context_ref2( struct md_download_context* dlctx, char const* file, int lineno ) {

   pthread_mutex_lock( &dlctx->finalize_lock );

   dlctx->ref_count++;
   SG_debug("download %p ref %d (from %s:%d)\n", dlctx, dlctx->ref_count, file, lineno );

   pthread_mutex_unlock( &dlctx->finalize_lock );
   return 0;
}


/**
 * @brief Unreference this download context
 *
 * If the reference count reaches 0, then free it
 * @retval 0 Success
 * @retval 1 Success, in which case the caller should follow up this call with a call to md_download_context_free.
 */
int md_download_context_unref2( struct md_download_context* dlctx, char const* file, int lineno ) {

   pthread_mutex_lock( &dlctx->finalize_lock );

   dlctx->ref_count--;
   SG_debug("download %p ref %d (from %s:%d)\n", dlctx, dlctx->ref_count, file, lineno );

   if( dlctx->ref_count <= 0 ) {

      dlctx->ref_count = 0;

      pthread_mutex_unlock( &dlctx->finalize_lock );

      return 1;
   }

   pthread_mutex_unlock( &dlctx->finalize_lock );
   return 0;
}


/**
 * @brief Unref, and possibly free a download context if it's fully unref'ed
 * @retval 0 Success
 * @retval 1 Success and free
 */
int md_download_context_unref_free( struct md_download_context* dlctx, CURL** ret_curl ) {

   int rc = md_download_context_unref( dlctx );
   if( rc > 0 ) {

      md_download_context_free( dlctx, ret_curl );
   }

   return rc;
}


/**
 * @brief Remove a download context if it is part of a download set
 * @note Always succeeds
 * @return 0
 */
int md_download_context_clear_set( struct md_download_context* dlctx ) {

   if( dlctx != NULL && dlctx->dlset != NULL ) {
      md_download_set_clear( dlctx->dlset, dlctx );
      dlctx->dlset = NULL;
   }

   return 0;
}

/**
 * @brief Wait for a download semaphore to be signaled, up to a given number of milliseconds.
 *
 * If timeout_ms is less than or equal to 0, then wait indefinitely.
 * If timeout_ms <= 0, then use sem_wait.  Otherwise, use sem_trywait +timeout_ms seconds into the future.
 * @note This method masks -EINTR.
 * @retval 0 Success
 * @retval -ETIMEDOUT Timed out waiting
 * @retval -errno sem_timedwait(2) or sem_wait(2) failed for some other reason
 */
int md_download_sem_wait( sem_t* sem, int64_t timeout_ms ) {

   int rc = 0;

   // do we timeout the wait?
   if( timeout_ms > 0 ) {

      struct timespec abs_ts;
      clock_gettime( CLOCK_REALTIME, &abs_ts );
      abs_ts.tv_sec += timeout_ms / 1000L;
      abs_ts.tv_nsec += timeout_ms / 1000000L;

      if( abs_ts.tv_nsec >= 1000000000L) {

         // wrap around
         abs_ts.tv_nsec %= 1000000000L;
         abs_ts.tv_sec ++;
      }

      while( true ) {

         rc = sem_timedwait( sem, &abs_ts );
         if( rc == 0 ) {

            break;
         }
         else if( errno != EINTR ) {

            rc = -errno;
            SG_error("sem_timedwait errno = %d\n", rc );
            break;
         }

         // otherwise, try again if interrupted
      }
   }
   else {
      while( true ) {

         rc = sem_wait( sem );
         if( rc == 0 ) {

            break;
         }
         else if( errno != EINTR ) {

            rc = -errno;
            SG_error("sem_wait errno = %d\n", rc );
            break;
         }

         // otherwise, try again if interrupted
      }
   }

   return rc;
}


/**
 * @brief Wait for a download to finish, either in error or not
 * @return The result of waiting, NOT the result of the download (see md_download_sem_wait for possible return codes)
 * @retval 0 Success
 * @retval -ETIMEDOUT timeout_ms is >= 0 and the deadline was exceeded
 */
int md_download_context_wait( struct md_download_context* dlctx, int64_t timeout_ms ) {

   SG_debug("Wait on download context %p (%" PRIu64 " millis)\n", dlctx, timeout_ms );

   int rc = md_download_sem_wait( &dlctx->sem, timeout_ms );

   if( rc != 0 ) {
   }
   SG_error("md_download_sem_wait rc = %d\n", rc );
   return rc;
}


/**
 * @brief Wait for a download to finish within a download set, either in error or not.
 * @return The result of waiting, NOT the result of the download (see md_downlaod_sem_wait for possible return codes)
 * @retval 0 Success
 * @retval -EINVAL The download set does not have any waiting contexts
 */
int md_download_context_wait_any( struct md_download_set* dlset, int64_t timeout_ms ) {

   if( dlset->waiting == NULL ) {
      return -EINVAL;
   }

   if( dlset->waiting->size() == 0 ) {
      return 0;
   }

   SG_debug("Wait on download set %p (%zu contexts)\n", dlset, dlset->waiting->size() );

   int rc = 0;

   // wait for at least one of them to finish
   rc = md_download_sem_wait( &dlset->sem, timeout_ms );

   if( rc != 0 ) {
      SG_error("md_download_sem_wait rc = %d\n", rc );
   }

   return rc;
}


/**
 * @brief Set up a download set
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 */
int md_download_set_init( struct md_download_set* dlset ) {

   SG_debug("Initialize download set %p\n", dlset );

   dlset->waiting = SG_safe_new( md_pending_set_t() );
   if( dlset->waiting == NULL ) {
      return -ENOMEM;
   }

   sem_init( &dlset->sem, 0, 0 );

   return 0;
}


/**
 * @brief Free a download set
 * @note Not thread safe
 * @retval 0 Success
 */
int md_download_set_free( struct md_download_set* dlset ) {

   SG_debug("Free download set %p\n", dlset );

   if( dlset->waiting ) {

      SG_safe_delete( dlset->waiting );
   }

   sem_destroy( &dlset->sem );

   memset( dlset, 0, sizeof( struct md_download_set ) );

   return 0;
}


/**
 * @brief Add a download context to a download set.
 *
 * @note Do this before starting the download, does not affect the download's reference count
 * @note Not thread safe
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 */
int md_download_set_add( struct md_download_set* dlset, struct md_download_context* dlctx ) {

   int rc = 0;

   try {

      md_pending_set_t::iterator itr = dlset->waiting->find( dlctx );
      if( itr == dlset->waiting->end() ) {

         dlset->waiting->insert( dlctx );

         dlctx->dlset = dlset;

         SG_debug("Add download context %p to download set %p\n", dlctx, dlset );
      }
   }
   catch( bad_alloc& ba ) {
      rc = -ENOMEM;
   }

   return rc;
}


/**
 * @brief Remove a download context from a download set by value
 * @note Don't do this in e.g. a for() loop where you're iterating over download contexts
 * @note Not thread safe
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 */
int md_download_set_clear( struct md_download_set* dlset, struct md_download_context* dlctx ) {

   try {
      if( dlctx != NULL && dlset->waiting != NULL ) {
         dlset->waiting->erase( dlctx );
      }
   }
   catch( bad_alloc& ba ) {
      return -ENOMEM;
   }

   if( dlctx != NULL ) {
      dlctx->dlset = NULL;
   }

   return 0;
}


/**
 * @brief Get number of items in a download set
 * @note Not thread safe
 * @return The number of items
 */
size_t md_download_set_size( struct md_download_set* dlset ) {

   return dlset->waiting->size();
}

/**
 * @brief Get whether this download set contain the provided download
 * @note Not thread safe
 */
bool md_download_set_contains( struct md_download_set* dlset, struct md_download_context* dlctx ) {
   return dlset->waiting->count( dlctx ) > 0;
}

/**
 * @brief Start the iteration to the provided download set
 * @return An iterator to the waiting set
 */
md_download_set_iterator md_download_set_begin( struct md_download_set* dlset ) {
   return dlset->waiting->begin();
}

/**
 * @brief Stop the iteration to the provided download set
 * @return An interator to the waiting set
 */
md_download_set_iterator md_download_set_end( struct md_download_set* dlset ) {
   return dlset->waiting->end();
}

/**
 * @brief Dereference the provided download set iterator
 * @return A pointer to the download context
 */
struct md_download_context* md_download_set_iterator_get_context( const md_download_set_iterator& itr ) {
   return *itr;
}


/**
 * @brief Begin downloading something
 *
 * This will ref the download (i.e. use it in conjunction with md_download_loop_watch)
 * @retval 0 Success
 * @retval !0 Failed to insert the download into the downloader
 */
int md_download_context_start( struct md_downloader* dl, struct md_download_context* dlctx ) {

   md_download_context_ref( dlctx );

   // enqueue the context into the downloader
   int rc = md_downloader_insert_pending( dl, dlctx );
   if( rc != 0 ) {

      SG_error("%s: md_downloader_insert_pending( %p ) rc = %d\n", dl->name, dlctx, rc );
      return rc;
   }

   return 0;
}

/**
 * @brief Cancel download
 * @retval 0 Success, or the download is already cancelled or finalized
 * @retval -EPERM The downloader was not running
 * @retval <0 Failed to insert (but mask EINPROGRESS) and/or wait for the download to get cancelled
 */
int md_download_context_cancel( struct md_downloader* dl, struct md_download_context* dlctx ) {

   if( !dl->running ) {
      return -EPERM;
   }

   pthread_mutex_lock( &dlctx->finalize_lock );

   if( dlctx->cancelled || dlctx->finalized || dlctx->cancelling ) {

      pthread_mutex_unlock( &dlctx->finalize_lock );

      SG_debug("already cancelled %p\n", dlctx );
      return 0;
   }

   struct md_download_set* dlset = dlctx->dlset;

   pthread_mutex_unlock( &dlctx->finalize_lock );

   int rc = md_downloader_insert_cancelling( dl, dlctx );

   if( rc != 0 && rc != -EINPROGRESS ) {

      // should only happen if the downloader isn't running
      SG_error("md_downloader_insert_cancelling(%p) rc = %d\n", dlctx, rc );
      return rc;
   }
   else {

      // EINPROGRESS is okay
      rc = 0;
   }

   SG_debug("Wait for cancelling download %p (set %p)\n", dlctx, dlset );
   rc = md_download_context_wait( dlctx, -1 );
   if( rc != 0 ) {

      // should only happen if the semaphore is invalid
      SG_error("md_download_context_wait(%p) rc = %d\n", dlctx, rc );
   }
   else {
      SG_debug("cancelled %p\n", dlctx );
   }

   return rc;
}


/**
 * @brief Release a waiting context set, given one of it's now-finished entries.
 * @retval 0 Success
 * @retval -EINVAL The download set is NULL
 */
int md_download_set_wakeup( struct md_download_set* dlset ) {

   SG_debug("Wake up download set %p\n", dlset );

   int rc = 0;

   if( dlset == NULL ) {
      return -EINVAL;
   }

   sem_post( &dlset->sem );

   return rc;
}


/**
 * @brief Run multiple downloads
 *
 * @note Download must be write-locked for downloading
 * @retval >0 Curl failed
 * @retval <0 select() failed
 */
int md_downloader_run_multi( struct md_downloader* dl ) {

   int still_running = 0;
   int rc = 0;
   struct timeval timeout;

   fd_set fdread;
   fd_set fdwrite;
   fd_set fdexcep;

   FD_ZERO( &fdread );
   FD_ZERO( &fdwrite );
   FD_ZERO( &fdexcep );

   int maxfd = -1;

   long curl_timeo = -1;

   // download for a bit
   rc = curl_multi_perform( dl->curlm, &still_running );
   if( rc != 0 ) {

      SG_error("%s: curl_multi_perform rc = %d\n", dl->name, rc );
      return rc;
   }

   // don't wait more than 5ms
   timeout.tv_sec = 0;
   timeout.tv_usec = 5000;      // 5ms

   rc = curl_multi_timeout( dl->curlm, &curl_timeo );
   if( rc != 0 ) {

      SG_error("%s: curl_multi_timeout rc = %d\n", dl->name, rc );
      return rc;
   }

   if( curl_timeo > 0 ) {
      timeout.tv_sec = curl_timeo / 1000;
      if( timeout.tv_sec > 0 ) {
         timeout.tv_sec = 0;
      }

      // no more than 5ms
      timeout.tv_usec = MIN( (curl_timeo % 1000) * 1000, 5000 );
   }

   // get fd set
   rc = curl_multi_fdset( dl->curlm, &fdread, &fdwrite, &fdexcep, &maxfd );
   if( rc != 0 ) {

      SG_error("%s: curl_multi_fdset rc = %d\n", dl->name, rc );
      return rc;
   }

   // select on them
   rc = select( maxfd + 1, &fdread, &fdwrite, &fdexcep, &timeout );
   if( rc < 0 ) {

      rc = -errno;
      SG_error("%s: select rc = %d\n", dl->name, rc );
      return rc;
   }
   else {
      rc = 0;
   }
   return rc;
}


/**
 * @brief Finalize a download context.
 *
 * On success, populate it with the curl rc, the HTTP status, the transfer OS errno, and the effective URL
 * http_status == -1 means that we couldn't get the HTTP status
 * os_errno == EIO means that we couldn't get the OS errno or the URL
 * @retval 0 Success, or if the download was already finalized (if the latter, then the fields in dlctx are untouched)
 * @retval 1 Success, in which case the caller should free the download context
 * @retval -ENOMEM Out of Memory
 */
int md_downloader_finalize_download_context( struct md_download_context* dlctx, int curl_rc ) {

   pthread_mutex_lock( &dlctx->finalize_lock );

   // sanity check
   if( md_download_context_finalized( dlctx ) ) {

      SG_debug("Download context %p already finalized\n", dlctx );
      pthread_mutex_unlock( &dlctx->finalize_lock );
      return 0;
   }

   int rc = 0;

   // check HTTP code
   long http_status = 0;
   long os_errno = 0;
   char* url = NULL;
   char* url_dup = NULL;

   rc = curl_easy_getinfo( dlctx->curl, CURLINFO_RESPONSE_CODE, &http_status );
   if( rc != 0 ) {

      SG_error("curl_easy_getinfo(%p) rc = %d\n", dlctx, rc );
      http_status = -1;
   }

   // check error code
   if( rc != 0 ) {
      rc = curl_easy_getinfo( dlctx->curl, CURLINFO_OS_ERRNO, &os_errno );

      if( rc != 0 ) {
         SG_error("curl_easy_getinfo(%p) rc = %d\n", dlctx, rc );
         os_errno = EIO;
      }
   }

   // get URL
   rc = curl_easy_getinfo( dlctx->curl, CURLINFO_EFFECTIVE_URL, &url );

   if( rc != 0 || url == NULL ) {
      SG_error("curl_easy_getinfo(%p) rc = %d\n", dlctx, rc );
      os_errno = EIO;
   }

   if( url != NULL ) {
      url_dup = SG_strdup_or_null( url );
      if( url_dup == NULL ) {

         os_errno = -ENOMEM;
      }
   }

   dlctx->curl_rc = curl_rc;
   dlctx->http_status = (int)http_status;
   dlctx->transfer_errno = (int)os_errno;
   dlctx->effective_url = NULL;

   if( url_dup != NULL ) {
      dlctx->effective_url = url_dup;

      SG_debug("Finalized download context %p (%s)\n", dlctx, dlctx->effective_url );
   }
   else {
      SG_debug("Finalized download context %p\n", dlctx );
   }

   dlctx->finalized = true;
   dlctx->running = false;

   // unreferenced
   dlctx->ref_count--;
   SG_debug("download %p ref %d\n", dlctx, dlctx->ref_count );

   if( dlctx->ref_count <= 0 ) {

      // caller should free
      rc = 1;
   }

   // wake up threads waiting for this download set
   if( dlctx->dlset != NULL ) {

      rc = md_download_set_wakeup( dlctx->dlset );
      if( rc != 0 ) {

         SG_error("md_download_set_wakeup( %p ) rc = %d\n", dlctx->dlset, rc );
         rc = 0;
      }
   }

   // wake up threads waitin for this specific download
   sem_post( &dlctx->sem );

   pthread_mutex_unlock( &dlctx->finalize_lock );

   return rc;
}


/**
 * @brief Finalize all finished downloads
 *
 * Try to remove all downloads, even if we fail to remove some.
 * @note Download must be write-locked for downloads
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 * @retval positive if curl did something fatal
 */
int md_downloader_finalize_download_contexts( struct md_downloader* dl ) {
   CURLMsg* msg = NULL;
   int msgs_left = 0;
   int rc = 0;

   do {
      msg = curl_multi_info_read( dl->curlm, &msgs_left );

      if( msg == NULL ) {
         // no messages
         break;
      }

      if( msg->msg == CURLMSG_DONE ) {
         // a transfer finished.  Find out which one
         md_downloading_map_t::iterator itr = dl->downloading->find( msg->easy_handle );
         if( itr != dl->downloading->end() ) {

            // found!
            struct md_download_context* dlctx = itr->second;

            // get this now, before removing it from the curlm handle
            int result = msg->data.result;

            try {
               // remove from the downloader
               dl->downloading->erase( itr );
            }
            catch( bad_alloc& ba ) {
               rc = -ENOMEM;
               break;
            }

            if( dlctx == NULL ) {

               SG_warn("no download context for curl handle %p\n", msg->easy_handle);

               rc = curl_multi_remove_handle( dl->curlm, msg->easy_handle );
               if( rc != 0 ) {

                  SG_error("curl_multi_remove_handle( %p ) rc = %d\n", msg->easy_handle, rc );
                  rc = 0;
               }

               continue;
            }

            if( dlctx->curl == NULL ) {
               SG_error("BUG: curl handle of download context %p is NULL\n", dlctx );

               rc = curl_multi_remove_handle( dl->curlm, msg->easy_handle );
            }
            else {
               rc = curl_multi_remove_handle( dl->curlm, dlctx->curl );
               if( rc != 0 ) {
                  SG_error("curl_multi_remove_handle(%p) rc = %d\n", msg->easy_handle, rc );
                  rc = 0;
               }
            }

            // get the download set from this dlctx, so we can awaken it later
            // struct md_download_set* dlset = dlctx->dlset;

            // finalize the download context, unref'ing it
            rc = md_downloader_finalize_download_context( dlctx, result );
            if( rc < 0 ) {

               SG_error("%s: md_downloader_finalize_download_context rc = %d\n", dl->name, rc );
               rc = 0;
               continue;
            }

            if( rc > 0 ) {

               // this was the last reference to the download context.  We should free it.
               CURL* curl = NULL;
               md_download_context_free( dlctx, &curl );
               curl_easy_cleanup( curl );
               SG_safe_free( dlctx );       // allowed, since dlctx can only be heap-allocated

               rc = 0;
            }

            /*
            // wake up the set waiting on this dlctx
            if( dlset != NULL ) {

               rc = md_download_set_wakeup( dlset );
               if( rc != 0 ) {

                  SG_error("md_download_set_wakeup( %p ) rc = %d\n", dlset, rc );
                  rc = 0;
                  continue;
               }
            }
            */
         }
      }

   } while( msg != NULL );

   return rc;
}


/**
 * @brief Main downloader loop
 * @return NULL
 */
static void* md_downloader_main( void* arg ) {

   struct md_downloader* dl = (struct md_downloader*)arg;

   SG_debug("%s: starting\n", dl->name );

   int rc = 0;

   while( dl->running ) {

      md_downloader_downloading_wlock( dl );

      // add all pending downloads to this downloader
      rc = md_downloader_start_all_pending( dl );
      if( rc != 0 ) {
         SG_error("%s: md_downloader_start_all_pending rc = %d\n", dl->name, rc );
      }

      // remove all cancelled downloads from this downloader
      rc = md_downloader_end_all_cancelling( dl );
      if( rc != 0 ) {
         SG_error("%s: md_downloader_end_all_cancelling rc = %d\n", dl->name, rc );
      }

      // download for a bit
      rc = md_downloader_run_multi( dl );
      if( rc != 0 ) {
         SG_error("%s: md_downloader_run_multi rc = %d\n", dl->name, rc );
      }

      // finalize any completed downloads
      rc = md_downloader_finalize_download_contexts( dl );
      if( rc != 0 ) {
         SG_error("%s: md_downloader_finalize_download_contexts rc = %d\n", dl->name, rc );
      }

      md_downloader_downloading_unlock( dl );

      // give the md_downloader_stop() method a chance to preempt the main method
   }

   SG_debug("%s: exiting\n", dl->name );
   return NULL;
}


/**
 * @brief Consolidate and write back the buffer
 * @retval 0 Success, and set *buf and *buf_len accordingly
 * @retval -ENOMEM Out of Memory
 */
int md_download_context_get_buffer( struct md_download_context* dlctx, char** buf, off_t* buf_len ) {

   *buf = md_response_buffer_to_string( dlctx->brb.rb );
   *buf_len = md_response_buffer_size( dlctx->brb.rb );

   if( *buf == NULL ) {
      return -ENOMEM;
   }

   return 0;
}

/**
 * @brief Get the http status
 * @return The positive HTTP status, Success
 * @retval -EAGAIN The download context was not finalized
 */
int md_download_context_get_http_status( struct md_download_context* dlctx ) {

   if( !dlctx->finalized ) {

      return -EAGAIN;
   }

   return dlctx->http_status;
}

/**
 * @brief Get the errno
 * @return The transfer error code (non-negative number) on sucess
 * @retval -EAGAIN The download context was not finalized
 */
int md_download_context_get_errno( struct md_download_context* dlctx ) {
   if( !dlctx->finalized ) {
      return -EAGAIN;
   }

   return dlctx->transfer_errno;
}

/**
 * @brief Get the curl rc
 * @retval >=0 Success
 * @retval -EAGAIN The download was not finalized
 */
int md_download_context_get_curl_rc( struct md_download_context* dlctx ) {
   if( !dlctx->finalized ) {
      return -EAGAIN;
   }

   return dlctx->curl_rc;
}

/**
 * @brief Get the effective URL
 * @retval 0 Success, and set *url to a malloc'ed copy of the URL (if there was a URL to begin with)
 * @retval -ENOMEM Out of Memory
 * @retval -EAGAIN The download was not finalized
 */
int md_download_context_get_effective_url( struct md_download_context* dlctx, char** url ) {
   if( !dlctx->finalized ) {
      return -EAGAIN;
   }

   if( dlctx->effective_url == NULL ) {
      *url = NULL;
   }
   else {

      *url = SG_strdup_or_null( dlctx->effective_url );
      if( *url == NULL ) {
         return -ENOMEM;
      }
   }

   return 0;
}

/**
 * @brief Get the download handle's curl handle
 * @return The curl handle
 */
CURL* md_download_context_get_curl( struct md_download_context* dlctx ) {
   return dlctx->curl;
}

/**
 * @brief Get the download handle's cls
 * @return Pointer to the cls
 */
void* md_download_context_get_cls( struct md_download_context* dlctx ) {
   return dlctx->cls;
}

/**
 * @brief Set the donwload handle's cls
 * @note Not thread-safe; only use it if you know what you're doing!
 * @note Always succeeds
 */
void md_download_context_set_cls( struct md_download_context* dlctx, void* new_cls ) {
   dlctx->cls = new_cls;
}

/**
 * @brief State of download context
 *
 * Check if the download context worked
 * @retval True The download's curl rc is 0, its transfer errno is 0, and its HTTP status is the desired HTTP status
 * @retval False Otherwise
 */
bool md_download_context_succeeded( struct md_download_context* dlctx, int desired_HTTP_status ) {
   return (dlctx->curl_rc == 0 && dlctx->transfer_errno == 0 && dlctx->http_status == desired_HTTP_status);
}

/**
 * @brief Check if download is finalized
 * @retval True Finalized
 * @retval False Not finalized
 */
bool md_download_context_finalized( struct md_download_context* dlctx ) {
   return dlctx->finalized;
}

/**
 * @brief Check if download is running
 * @retval True Running
 * @retval False Not running
 */
bool md_download_context_running( struct md_download_context* dlctx ) {
   return dlctx->running;
}

/**
 * @brief Check if download is pending
 * @retval True Pending
 * @retval False Not pending
 */
bool md_download_context_pending( struct md_download_context* dlctx ) {
   return dlctx->pending;
}

/**
 * @brief Check if download was cancelled
 * @retval True Cancelled
 * @retval False Not cancelled
 */
bool md_download_context_cancelled( struct md_download_context* dlctx ) {
   return dlctx->cancelled;
}

/**
 * @brief Check if a download is initialized
 * @retval True Initialized
 * @retval False Not initialized
 */
bool md_download_context_initialized( struct md_download_context* dlctx ) {
   return dlctx->initialized;
}

/**
 * @brief Run a single download context
 * @note dlctx *cannot* be locked
 * @retval 0 Successful finalization, even if the download failed
 * @retval 1 Successful finalization, even if the download failed, in which case the caller should free the download context since it's been fully unref'ed
 * @retval <0 Otherwise (see md_downloader_finalize_download_context)
 */
int md_download_context_run( struct md_download_context* dlctx ) {

   int rc = 0;

   dlctx->running = true;

   rc = curl_easy_perform( dlctx->curl );
   if( rc != 0 ) {

      SG_error("curl_easy_perform( %p ) rc = %d\n", dlctx, rc );
   }

   rc = md_downloader_finalize_download_context( dlctx, rc );

   return rc;
}

/**
 * @brief Set sockopt for curl
 * @todo Test various flags for curl sockopt, TCP_CORK or TCP_NODELAY
 * @retval CURL_SOCKOPT_OK
 */
static int md_curl_sockopt( void* userdata, curl_socket_t sockfd, curlsocktype purpose ) {

   int rc = CURL_SOCKOPT_OK;

   if( purpose == CURLSOCKTYPE_IPCXN ) {

      // opening a socket
      // TODO: experiment with different flags here--maybe TCP_CORK or TCP_NODELAY
      rc = CURL_SOCKOPT_OK;
   }

   return rc;
}


/**
 * @brief Initialze a curl handle
 */
static void md_init_curl_handle2( CURL* curl_h, char const* url, time_t query_timeout, bool ssl_verify_peer, int64_t transfer_timeout ) {

   curl_easy_setopt( curl_h, CURLOPT_NOPROGRESS, 1L );   // no progress bar
   curl_easy_setopt( curl_h, CURLOPT_USERAGENT, "Syndicate-Gateway/1.0");

   if( url != NULL ) {
      curl_easy_setopt( curl_h, CURLOPT_URL, url );
   }

   curl_easy_setopt( curl_h, CURLOPT_FOLLOWLOCATION, 1L );
   curl_easy_setopt( curl_h, CURLOPT_MAXREDIRS, 10L );
   curl_easy_setopt( curl_h, CURLOPT_NOSIGNAL, 1L );
   curl_easy_setopt( curl_h, CURLOPT_CONNECTTIMEOUT, query_timeout );
   curl_easy_setopt( curl_h, CURLOPT_FILETIME, 1L );

   if( url != NULL && strncasecmp( url, "https", 5 ) == 0 ) {
      curl_easy_setopt( curl_h, CURLOPT_USE_SSL, CURLUSESSL_ALL );
      curl_easy_setopt( curl_h, CURLOPT_SSL_VERIFYPEER, ssl_verify_peer ? 1L : 0L );
      curl_easy_setopt( curl_h, CURLOPT_SSL_VERIFYHOST, 2L );
   }
   else {
      curl_easy_setopt( curl_h, CURLOPT_USE_SSL, CURLUSESSL_NONE );
      curl_easy_setopt( curl_h, CURLOPT_SSL_VERIFYPEER, 0L );
      curl_easy_setopt( curl_h, CURLOPT_SSL_VERIFYHOST, 0L );
   }

   curl_easy_setopt( curl_h, CURLOPT_SOCKOPTFUNCTION, md_curl_sockopt );

   // stop-gap
   curl_easy_setopt( curl_h, CURLOPT_TIMEOUT, transfer_timeout );

   //curl_easy_setopt( curl_h, CURLOPT_VERBOSE, 1L );
}


/**
 * @brief Initialze a curl handle
 * @note Always succeeds
 */
void md_init_curl_handle( struct md_syndicate_conf* conf, CURL* curl_h, char const* url, time_t query_timeout ) {
   md_init_curl_handle2( curl_h, url, query_timeout, conf->verify_peer, conf->transfer_timeout );
}


/**
 * @brief Interpret error messages from a download context into an appropriate return code for the downloader.
 * @retval 0 Success
 * @retval -EAGAIN The download should be retried
 * @retval -ETIMEDOUT Operation timeout
 * @retval -EREMOTEIO The HTTP status was >= 500, or an indeterminate error occurred but errno was not set.
 * @retval -http_status The HTTP status was between 400 and 499
 */
int md_download_interpret_errors( int http_status, int curl_rc, int os_err ) {

   int rc = 0;

   if( http_status == SG_HTTP_TRYAGAIN ) {

      return -EAGAIN;
   }

   if( curl_rc == CURLE_OPERATION_TIMEDOUT || os_err == -ETIMEDOUT || curl_rc == CURLE_GOT_NOTHING ) {

      return -ETIMEDOUT;
   }

   // serious error?
   if( http_status >= 500 ) {

      return -EREMOTEIO;
   }

   // some other error?
   if( http_status != 200 || curl_rc != 0 ) {

      if( http_status >= 400 && http_status <= 499 ) {
         rc = -http_status;
      }
      else if( os_err != 0 ) {
         rc = -os_err;
      }
      else {
         rc = -EREMOTEIO;
      }

      return rc;
   }

   return 0;
}


/**
 * @brief Translate an HTTP status code into the approprate error code.
 * @return The error or status code
 */
int md_HTTP_status_code_to_error_code( int status_code ) {
   if( status_code == SG_HTTP_TRYAGAIN ) {
      return -EAGAIN;
   }

   if( status_code == 500 ) {
      return -EREMOTEIO;
   }

   if( status_code == 404 ) {
      return -ENOENT;
   }

   return status_code;
}


/**
 * @brief Alloc a download loop
 * @return Pointer to the allocated memory block
 */
struct md_download_loop* md_download_loop_new() {
   return SG_CALLOC( struct md_download_loop, 1 );
}

/**
 * @brief Initialize a download loop
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 */
int md_download_loop_init( struct md_download_loop* dlloop, struct md_downloader* dl, int num_downloads ) {

   memset( dlloop, 0, sizeof(struct md_download_loop) );

   dlloop->downloads = SG_CALLOC( struct md_download_context*, num_downloads );
   if( dlloop->downloads == NULL ) {

      return -ENOMEM;
   }

   for( int i = 0; i < num_downloads; i++ ) {
      dlloop->downloads[i] = SG_CALLOC( struct md_download_context, 1 );
      if( dlloop->downloads[i] == NULL ) {

         // clean up
         for( int j = 0; j < i; j++ ) {
            SG_safe_free( dlloop->downloads[i] );
         }

         SG_safe_free( dlloop->downloads );
         return -ENOMEM;
      }
   }

   int rc = md_download_set_init( &dlloop->dlset );
   if( rc != 0 ) {

      // clean up
      for( int j = 0; j < num_downloads; j++ ) {
         SG_safe_free( dlloop->downloads[j] );
      }
      SG_safe_free( dlloop->downloads );
      return rc;
   }

   dlloop->num_downloads = num_downloads;
   dlloop->dl = dl;

   return 0;
}


/**
 * @brief Free a download loop
 * @note Always succeeds
 */
int md_download_loop_free( struct md_download_loop* dlloop ) {

   for( int i = 0; i < dlloop->num_downloads; i++ ) {

      if( !md_download_context_initialized( dlloop->downloads[i] ) ) {
         SG_safe_free( dlloop->downloads[i] );
      }
   }

   SG_safe_free( dlloop->downloads );
   md_download_set_free( &dlloop->dlset );

   memset( dlloop, 0, sizeof(struct md_download_loop) );

   return 0;
}


/**
 * @brief Get the next available download in the download loop.
 * @retval 0 Success, and set *dlctx to point to the available download
 * @retval -EAGAIN No free downloads, and set *dlctx to NULL
 */
int md_download_loop_next( struct md_download_loop* dlloop, struct md_download_context** dlctx ) {

   *dlctx = NULL;
   for( int i = 0; i < dlloop->num_downloads; i++ ) {

      if( !md_download_context_initialized( dlloop->downloads[i] ) ) {

         *dlctx = dlloop->downloads[i];

         return 0;
      }
   }

   return -EAGAIN;
}


/**
 * @brief Have the download loop process the download when it runs
 *
 * md_download_loop_run knows to return if the given download finishes.
 * @return Status of running md_download_set_add
 * @see md_download_set_add
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 */
int md_download_loop_watch( struct md_download_loop* dlloop, struct md_download_context* dlctx ) {
   return md_download_set_add( &dlloop->dlset, dlctx );
}


/**
 * @brief Run the download loop, until at least one download completes
 * @retval 0 Success
 * @retval 1 if there are no more downloads
 * @retval -errno on critical failure to wait
 */
int md_download_loop_run( struct md_download_loop* dlloop ) {

   int rc = 0;
   dlloop->started = true;

   while( md_download_set_size( &dlloop->dlset ) > 0 ) {

      // wait for some downloads to finish, but be resillent against deadlock
      rc = md_download_context_wait_any( &dlloop->dlset, 10000 );
      if( rc != 0 && rc != -ETIMEDOUT ) {

         // failed
         SG_error("md_download_context_wait_any(%p) rc = %d\n", &dlloop->dlset, rc );
         return rc;
      }

      else if( rc == -ETIMEDOUT ) {

         SG_debug("still waiting on download set %p\n", &dlloop->dlset );
         continue;
      }

      return rc;
   }

   return 1;
}


/**
 * @brief Find a finished download
 * @note The caller must unref and free when done with it
 * @retval 0 Success, and set *dlctx to point to the finished download
 * @retval -EAGAIN if there are no finished downloads, and set *dlctx to NULL
 */
int md_download_loop_finished( struct md_download_loop* dlloop, struct md_download_context** dlctx ) {

   *dlctx = NULL;
   for( int i = 0; i < dlloop->num_downloads; i++ ) {

      if( md_download_context_initialized( dlloop->downloads[i] ) && md_download_context_finalized( dlloop->downloads[i] ) && md_download_set_contains( &dlloop->dlset, dlloop->downloads[i] ) ) {

         *dlctx = dlloop->downloads[i];
         md_download_set_clear( &dlloop->dlset, *dlctx );

         return 0;
      }
   }

   return -EAGAIN;
}


/**
 * @brief Get the number of running downloads
 * @retval >=0 Number of downloads running
 */
int md_download_loop_num_running( struct md_download_loop* dlloop ) {

   int ret = 0;

   for( int i = 0; i < dlloop->num_downloads; i++ ) {

      if( md_download_context_running( dlloop->downloads[i] ) ) {

         ret++;
      }
   }

   return ret;
}


/**
 * @brief Get the number of initialized downloads
 * @retval >=0 Number of initialized downloads
 */
int md_download_loop_num_initialized( struct md_download_loop* dlloop ) {

   int ret = 0;

   for( int i = 0; i < dlloop->num_downloads; i++ ) {

      if( md_download_context_initialized( dlloop->downloads[i] ) ) {

         ret++;
      }
   }

   return ret;
}

/**
 * @brief Determine if there are downloads outstanding
 * @retval True Outstanding downloads
 * @retval False No outstanding downloads
 */
bool md_download_loop_running( struct md_download_loop* dlloop ) {

   return (dlloop->started && md_download_loop_num_initialized( dlloop ) > 0);
}


/**
 * @brief Cancel all downloads in a download loop, but in a fail-fast manner
 * @retval 0 Success
 * @retval -errno Failure to cancel (see md_download_context_cancel)
 * @see md_download_context_cancel
 */
int md_download_loop_abort( struct md_download_loop* dlloop ) {

   int rc = 0;
   struct md_download_context* dlctx = NULL;

   for( int i = 0; i < dlloop->num_downloads; i++ ) {

      dlctx = dlloop->downloads[i];

      if( !dlctx->initialized ) {
         continue;
      }

      // cancel it
      rc = md_download_context_cancel( dlloop->dl, dlctx );
      if( rc != 0 ) {

         SG_error("md_download_context_cancel( %p ) rc = %d\n", dlctx, rc );
         break;
      }
   }

   return rc;
}


/**
 * @brief Unreference all downloads in a download loop
 *
 * Free them and pass their CURL handles to curl_release if their ref counts reach 0
 * If given, call the curl release function on each CURL handle
 * @note All the download contexts here have to be ref'ed!
 * @note Always succeeds
 */
int md_download_loop_cleanup( struct md_download_loop* dlloop, md_download_curl_release_func curl_release, void* release_cls ) {

   int rc = 0;
   struct md_download_context* dlctx = NULL;
   CURL* curl = NULL;

   SG_debug("Clean up download loop %p (set %p)\n", dlloop, &dlloop->dlset);

   for( int i = 0; i < dlloop->num_downloads; i++ ) {

      dlctx = dlloop->downloads[i];

      if( !dlctx->initialized ) {
         // clear from parent set
         SG_debug("Clear download %p from set %p in loop %p\n", dlctx, &dlloop->dlset, dlloop );
         md_download_set_clear( &dlloop->dlset, dlctx );
         continue;
      }
      else {
         // remove from parent set
         md_download_context_clear_set( dlctx );
      }

      // wait to finish
      SG_debug("Wait for download context %p (set %p, loop %p)\n", dlctx, &dlloop->dlset, dlloop);
      md_download_context_wait( dlctx, -1 );

      // unref
      rc = md_download_context_unref( dlctx );
      if( rc > 0 ) {

         // fully unref'ed
         md_download_context_free( dlctx, &curl );

         if( curl_release != NULL ) {

            (*curl_release)( curl, release_cls );
         }
         else {

            // default behavior
            curl_easy_cleanup( curl );
         }
      }
   }

   return 0;
}

/**
 * @brief Find the next initialized download at the given offset, incrementing *i
 *
 * If i == NULL, just find the first initialized download
 * @return A pointer to the download, Success
 * @retval NULL Attempting to index beyond the array (i >= num_downloads)
 */
struct md_download_context* md_download_loop_next_initialized( struct md_download_loop* dlloop, int* i ) {

   if( i == NULL ) {
      for( int j = 0; j < dlloop->num_downloads; j++ ) {

         if( dlloop->downloads[j]->initialized ) {

            return dlloop->downloads[j];
         }
      }

      return NULL;
   }
   else {

      while( *i < dlloop->num_downloads && !dlloop->downloads[*i]->initialized ) {
         *i = (*i) + 1;
      }

      if( *i >= dlloop->num_downloads ) {
         return NULL;
      }
      else {
         struct md_download_context* ret = dlloop->downloads[*i];
         *i = (*i) + 1;
         return ret;
      }
   }
}

/**
 * @brief Download a single item, synchronously, up to max_size bytes (pass -1 for no maximum size)
 * @retval 0 Success, and populate *buf and *buflen with the downloaded data
 * @retval -ENOMEM Out of Memory
 * @retval -ETIMEDOUT The tranfser could not complete in time
 * @retval -EAGAIN Signaled to retry the request
 * @retval -EREMOTEIO HTTP error is >= 500
 * @retval -499<=rc>=-400 The HTTP error was in the range 400 to 499
 * @retval Other -errno on socket- and recv-related errors
 */
int md_download_run( CURL* curl, off_t max_size, char** buf, off_t* buf_len ) {

   int rc = 0;
   long http_status = 0;
   long os_errno = 0;
   struct md_bound_response_buffer brb;

   // initialize
   rc = md_bound_response_buffer_init( &brb, max_size );
   if( rc != 0 ) {

      return rc;
   }

   // point curl to our brb
   curl_easy_setopt( curl, CURLOPT_WRITEDATA, &brb );
   curl_easy_setopt( curl, CURLOPT_WRITEFUNCTION, md_get_callback_bound_response_buffer );

   rc = curl_easy_perform( curl );

   // get HTTP status and error code
   curl_easy_getinfo( curl, CURLINFO_RESPONSE_CODE, &http_status );
   curl_easy_getinfo( curl, CURLINFO_OS_ERRNO, &os_errno );

   if( rc != 0 || http_status >= 400 ) {

      SG_error("curl_easy_perform rc = %d, HTTP status = %ld, os_errno = %ld\n", rc, http_status, os_errno );

      rc = md_download_interpret_errors( http_status, rc, os_errno );

      // clean up
      md_bound_response_buffer_free( &brb );
      return rc;
   }

   // extract the buffer
   *buf = md_response_buffer_to_string( brb.rb );
   *buf_len = md_response_buffer_size( brb.rb );

   md_bound_response_buffer_free( &brb );

   if( *buf == NULL ) {

      rc = -ENOMEM;
   }

   return rc;
}


/**
 * @brief Initialize a bound response buffer
 * @retval 0 Success
 * @retval -ENOMEM if allocation failed
 */
int md_bound_response_buffer_init( struct md_bound_response_buffer* brb, off_t max_size ) {

   memset( brb, 0, sizeof(struct md_bound_response_buffer) );

   brb->rb = SG_safe_new( md_response_buffer_t() );
   if( brb == NULL ) {
      return -ENOMEM;
   }

   brb->max_size = max_size;
   brb->size = 0;

   return 0;
}

/**
 * @brief Free a bound response buffer
 * @note Always succeeds
 */
int md_bound_response_buffer_free( struct md_bound_response_buffer* brb ) {

   if( brb->rb != NULL ) {
      md_response_buffer_free( brb->rb );
      SG_safe_delete( brb->rb );
   }

   memset( brb, 0, sizeof(struct md_bound_response_buffer) );

   return 0;
}



// IYCHOI
struct md_download_connection* md_download_connection_new() {
    return SG_CALLOC( struct md_download_connection, 1 );
}

int md_download_connection_init( struct md_download_connection* dlconn, struct md_download_connection_pool* dlcpool, uint64_t gateway_id ) {
    int rc = 0;

    memset( dlconn, 0, sizeof(struct md_download_connection) );

    rc = pthread_rwlock_init( &dlconn->lock, NULL );
    if( rc != 0 ) {
       return -rc;
    }

    dlconn->pool = dlcpool;
    dlconn->gateway_id = gateway_id;

    // init CURL
    dlconn->curl = curl_easy_init();
    if( dlconn->curl == NULL ) {
       return -ENOMEM;
    }

    dlconn->inited = true;
    return 0;
}

int md_download_connection_free( struct md_download_connection* dlconn ) {
    if( !dlconn->inited ) {
       // not initialized
       return -EINVAL;
    }

    // destroy
    md_download_connection_wlock( dlconn );

    dlconn->inited = false;

    // uninit CURL
    curl_easy_cleanup( dlconn->curl );

    md_download_connection_unlock( dlconn );
    pthread_rwlock_destroy( &dlconn->lock );

    memset( dlconn, 0, sizeof(struct md_download_connection) );

    return 0;
}

int md_download_connection_wlock( struct md_download_connection* dlconn ) {
   return pthread_rwlock_wrlock( &dlconn->lock );
}

int md_download_connection_unlock( struct md_download_connection* dlconn ) {
   return pthread_rwlock_unlock( &dlconn->lock );
}

CURL* md_download_connection_get_curl( struct md_download_connection* dlconn ) {
    return dlconn->curl;
}

// connection group
struct md_download_connection_group* md_download_connection_group_new() {
    return SG_CALLOC( struct md_download_connection_group, 1 );
}

int md_download_connection_group_init( struct md_download_connection_group* dlcgroup ) {
    int rc = 0;

    memset( dlcgroup, 0, sizeof(struct md_download_connection_group) );

    rc = pthread_rwlock_init( &dlcgroup->lock, NULL );
    if( rc != 0 ) {
       return -rc;
    }

    dlcgroup->active = SG_safe_new( md_download_connection_set_t() );
    if(dlcgroup->active == NULL) {
        return -ENOMEM;
    }

    dlcgroup->idle = SG_safe_new( md_download_connection_queue_t() );
    if(dlcgroup->idle == NULL) {
        SG_safe_delete(dlcgroup->active);
        return -ENOMEM;
    }

    dlcgroup->inited = true;
    return 0;
}

int md_download_connection_group_free( struct md_download_connection_group* dlcgroup ) {
    if( !dlcgroup->inited ) {
       // not initialized
       return -EINVAL;
    }

    // destroy
    md_download_connection_group_wlock( dlcgroup );
    if(dlcgroup->active != NULL) {
        for(md_download_connection_set_t::iterator itr = dlcgroup->active->begin(); itr != dlcgroup->active->end(); itr++ ) {
            struct md_download_connection* dlconn = *itr;
            md_download_connection_free(dlconn);
            SG_safe_free(dlconn);
        }

        dlcgroup->active->clear();
        delete dlcgroup->active;
        dlcgroup->active = NULL;
    }

    if(dlcgroup->idle != NULL) {
        while(dlcgroup->idle->size() > 0) {
            struct md_download_connection* dlconn = dlcgroup->idle->front();
            dlcgroup->idle->pop();

            md_download_connection_free(dlconn);
            SG_safe_free(dlconn);
        }

        delete dlcgroup->idle;
        dlcgroup->idle = NULL;
    }
    md_download_connection_group_unlock( dlcgroup );
    pthread_rwlock_destroy( &dlcgroup->lock );

    memset( dlcgroup, 0, sizeof(struct md_download_connection_group) );

    return 0;
}

int md_download_connection_group_wlock( struct md_download_connection_group* dlcgroup ) {
   return pthread_rwlock_wrlock( &dlcgroup->lock );
}

int md_download_connection_group_rlock( struct md_download_connection_group* dlcgroup ) {
   return pthread_rwlock_rdlock( &dlcgroup->lock );
}

int md_download_connection_group_unlock( struct md_download_connection_group* dlcgroup ) {
   return pthread_rwlock_unlock( &dlcgroup->lock );
}

int md_download_connection_group_count_active( struct md_download_connection_group* dlcgroup ) {
    int count = 0;
    if( !dlcgroup->inited ) {
       // not initialized
       return -EINVAL;
    }

    md_download_connection_group_rlock( dlcgroup );
    count = dlcgroup->active->size();
    md_download_connection_group_unlock( dlcgroup );

    return count;
}

int md_download_connection_group_pop_active( struct md_download_connection_group* dlcgroup, struct md_download_connection* dlconn ) {
    if( !dlcgroup->inited ) {
       // not initialized
       SG_error("download connection group is not initialized for %p\n", dlcgroup);
       return -EINVAL;
    }

    md_download_connection_group_wlock( dlcgroup );
    dlcgroup->active->erase(dlconn);
    md_download_connection_group_unlock( dlcgroup );
    return 0;
}

int md_download_connection_group_push_active( struct md_download_connection_group* dlcgroup, struct md_download_connection* dlconn ) {
    int rc = 0;
    if( !dlcgroup->inited ) {
       // not initialized
       SG_error("download connection group is not initialized for %p\n", dlcgroup);
       return -EINVAL;
    }

    md_download_connection_group_wlock( dlcgroup );
    try {
        dlcgroup->active->insert(dlconn);
    }
    catch(bad_alloc& ba) {
        rc = -ENOMEM;
    }
    md_download_connection_group_unlock( dlcgroup );
    return rc;
}

int md_download_connection_group_count_idle( struct md_download_connection_group* dlcgroup ) {
    int count = 0;
    if( !dlcgroup->inited ) {
       // not initialized
       return -EINVAL;
    }

    md_download_connection_group_rlock( dlcgroup );
    count = dlcgroup->idle->size();
    md_download_connection_group_unlock( dlcgroup );

    return count;
}

struct md_download_connection* md_download_connection_group_pop_idle( struct md_download_connection_group* dlcgroup ) {
    struct md_download_connection* dlconn = NULL;

    if( !dlcgroup->inited ) {
       // not initialized
       SG_error("download connection group is not initialized for %p\n", dlcgroup);
       return NULL;
    }

    md_download_connection_group_wlock( dlcgroup );
    if(dlcgroup->idle->size() > 0) {
        dlconn = dlcgroup->idle->front();
        dlcgroup->idle->pop();
    }
    md_download_connection_group_unlock( dlcgroup );
    return dlconn;
}

int md_download_connection_group_push_idle( struct md_download_connection_group* dlcgroup, struct md_download_connection* dlconn ) {
    int rc = 0;
    if( !dlcgroup->inited ) {
       // not initialized
       SG_error("download connection group is not initialized for %p\n", dlcgroup);
       return -EINVAL;
    }

    md_download_connection_group_wlock( dlcgroup );
    try {
        dlcgroup->idle->push(dlconn);
    }
    catch(bad_alloc& ba) {
        rc = -ENOMEM;
    }
    md_download_connection_group_unlock( dlcgroup );
    return rc;
}

// connection pool
struct md_download_connection_pool* md_download_connection_pool_new() {
    return SG_CALLOC( struct md_download_connection_pool, 1 );
}

int md_download_connection_pool_init( struct md_download_connection_pool* dlcpool ) {
    int rc = 0;

    memset( dlcpool, 0, sizeof(struct md_download_connection_pool) );

    rc = pthread_rwlock_init( &dlcpool->lock, NULL );
    if( rc != 0 ) {
       return -rc;
    }

    dlcpool->connections = SG_safe_new( md_download_connection_pool_map_t() );
    if(dlcpool->connections == NULL) {
        return -ENOMEM;
    }

    dlcpool->user_data = NULL;
    dlcpool->event_func = NULL;
    dlcpool->inited = true;
    return 0;
}

int md_download_connection_pool_free( struct md_download_connection_pool* dlcpool ) {
    if( !dlcpool->inited ) {
       // not initialized
       return -EINVAL;
    }

    // destroy
    md_download_connection_pool_wlock( dlcpool );
    if(dlcpool->connections != NULL) {
        for( md_download_connection_pool_map_t::iterator itr = dlcpool->connections->begin(); itr != dlcpool->connections->end(); itr++ ) {
            struct md_download_connection_group* dlcgroup = itr->second;

            md_download_connection_group_free(dlcgroup);
            SG_safe_free(dlcgroup);
        }

        dlcpool->connections->clear();
        delete dlcpool->connections;
        dlcpool->connections = NULL;
    }
    md_download_connection_pool_unlock( dlcpool );
    pthread_rwlock_destroy( &dlcpool->lock );

    memset( dlcpool, 0, sizeof(struct md_download_connection_pool) );

    return 0;
}

int md_download_connection_pool_wlock( struct md_download_connection_pool* dlcpool ) {
   return pthread_rwlock_wrlock( &dlcpool->lock );
}

int md_download_connection_pool_rlock( struct md_download_connection_pool* dlcpool ) {
   return pthread_rwlock_rdlock( &dlcpool->lock );
}

int md_download_connection_pool_unlock( struct md_download_connection_pool* dlcpool ) {
   return pthread_rwlock_unlock( &dlcpool->lock );
}

int md_download_connection_pool_set_user_data( struct md_download_connection_pool* dlcpool, void* user_data) {
    if( !dlcpool->inited ) {
       // not initialized
       SG_error("download connection pool is not initialized for %p\n", dlcpool);
       return -EINVAL;
    }

    md_download_connection_pool_wlock( dlcpool );
    dlcpool->user_data = user_data;
    md_download_connection_pool_unlock( dlcpool );
    return 0;
}

void* md_download_connection_pool_get_user_data( struct md_download_connection_pool* dlcpool ) {
    void* user_data = NULL;
    if( !dlcpool->inited ) {
       // not initialized
       SG_error("download connection pool is not initialized for %p\n", dlcpool);
       return NULL;
    }

    md_download_connection_pool_rlock( dlcpool );
    user_data = dlcpool->user_data;
    md_download_connection_pool_unlock( dlcpool );
    return user_data;
}

int md_download_connection_pool_set_event_func( struct md_download_connection_pool* dlcpool, md_download_connection_pool_event_func func) {
    if( !dlcpool->inited ) {
       // not initialized
       SG_error("download connection pool is not initialized for %p\n", dlcpool);
       return -EINVAL;
    }

    md_download_connection_pool_wlock( dlcpool );
    dlcpool->event_func = func;
    md_download_connection_pool_unlock( dlcpool );
    return 0;
}

md_download_connection_pool_event_func md_download_connection_pool_get_event_func( struct md_download_connection_pool* dlcpool ) {
    md_download_connection_pool_event_func func = NULL;
    if( !dlcpool->inited ) {
       // not initialized
       SG_error("download connection pool is not initialized for %p\n", dlcpool);
       return NULL;
    }

    md_download_connection_pool_rlock( dlcpool );
    func = dlcpool->event_func;
    md_download_connection_pool_unlock( dlcpool );
    return func;
}

int md_download_connection_pool_call_event_func( struct md_download_connection_pool* dlcpool, uint32_t event_type, void* event_data) {
    md_download_connection_pool_event_func func = NULL;
    if( !dlcpool->inited ) {
       // not initialized
       SG_error("download connection pool is not initialized for %p\n", dlcpool);
       return -EINVAL;
    }

    md_download_connection_pool_rlock( dlcpool );
    func = dlcpool->event_func;
    if(func != NULL) {
        (*func)(dlcpool, event_type, event_data);
    }
    md_download_connection_pool_unlock( dlcpool );
    return 0;
}

struct md_download_connection* md_download_connection_pool_get( struct md_download_connection_pool* dlcpool, uint64_t gateway_id) {

    int rc = 0;
    struct md_download_connection_group* dlcgroup = NULL;
    struct md_download_connection* dlconn = NULL;

    if( !dlcpool->inited ) {
       // not initialized
       SG_error("download connection pool is not initialized for %p\n", dlcpool);
       return NULL;
    }

    md_download_connection_pool_wlock( dlcpool );

    md_download_connection_pool_map_t::iterator itr = dlcpool->connections->find( gateway_id );
    if( itr != dlcpool->connections->end() ) {
       // found!
       dlcgroup = itr->second;
       SG_debug("reuse existing download connection group for gateway id %" PRIX64 "\n", gateway_id);
    }

    if(dlcgroup == NULL) {
        SG_debug("no download connection group for gateway id %" PRIX64 " - create a new connection group \n", gateway_id);

        // make one and put to the pool
        dlcgroup = md_download_connection_group_new();
        rc = md_download_connection_group_init( dlcgroup );
        if( rc != 0) {
            SG_safe_free(dlcgroup);
            md_download_connection_pool_unlock( dlcpool );
            return NULL;
        }

        try {
            (*dlcpool->connections)[ gateway_id ] = dlcgroup;
        }
        catch( bad_alloc& ba ) {
            SG_safe_free(dlcgroup);
            rc = -ENOMEM;
            md_download_connection_pool_unlock( dlcpool );
            return NULL;
        }
    }

    dlconn = md_download_connection_group_pop_idle( dlcgroup );
    if(dlconn == NULL) {
        SG_debug("no download connection for gateway id %" PRIX64 " - create a new connection\n", gateway_id);

        // make one and put to the pool
        dlconn = md_download_connection_new();
        rc = md_download_connection_init( dlconn, dlcpool, gateway_id );
        if( rc != 0) {
            SG_safe_free(dlconn);
            md_download_connection_pool_unlock( dlcpool );
            return NULL;
        }

        // push to active
        rc = md_download_connection_group_push_active( dlcgroup, dlconn );
        if( rc != 0) {
            SG_safe_free(dlconn);
            md_download_connection_pool_unlock( dlcpool );
            return NULL;
        }
    } else {
        SG_debug("reuse existing download connection for gateway id %" PRIX64 ", conn %p\n", gateway_id, dlconn);
        // move to active
        rc = md_download_connection_group_push_active( dlcgroup, dlconn );
        if( rc != 0) {
            md_download_connection_pool_unlock( dlcpool );
            return NULL;
        }
    }

    md_download_connection_pool_unlock( dlcpool );
    return dlconn;
}

int md_download_connection_pool_make_idle( struct md_download_connection_pool* dlcpool, struct md_download_connection* dlconn ) {
    int rc = 0;
    struct md_download_connection_group* dlcgroup = NULL;

    if( !dlcpool->inited ) {
       // not initialized
       SG_error("download connection pool is not initialized for %p\n", dlcpool);
       return -EINVAL;
    }

    md_download_connection_pool_wlock( dlcpool );

    md_download_connection_pool_map_t::iterator itr = dlcpool->connections->find( dlconn->gateway_id );
    if( itr != dlcpool->connections->end() ) {
       // found!
       dlcgroup = itr->second;
       rc = md_download_connection_group_pop_active(dlcgroup, dlconn);
       if( rc != 0) {
           md_download_connection_pool_unlock( dlcpool );
           return rc;
       }

       SG_debug("make download connection idle for gateway id %" PRIX64 ", conn %p\n", dlconn->gateway_id, dlconn);
       rc = md_download_connection_group_push_idle(dlcgroup, dlconn);
       if( rc != 0) {
           md_download_connection_pool_unlock( dlcpool );
           return rc;
       }
    } else {
        SG_error("download connection group is not found for %" PRIX64 "\n", dlconn->gateway_id);
        return -EINVAL;
    }

    md_download_connection_pool_unlock( dlcpool );

    return 0;
}
// IYCHOI

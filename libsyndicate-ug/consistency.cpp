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
 * @file libsyndicate-ug/consistency.cpp
 * @author Jude Nelson
 * @date 9 Mar 2016
 *
 * @brief User Gateway consistency related functions
 *
 * @see libsyndicate-ug/consistency.h
 */

#include "consistency.h"
#include "read.h"

/// MS path entry context
struct UG_path_ent_ctx {

   char* fs_path;               ///< Path to this entry
   struct fskit_entry* fent;    ///< Entry
};

/// Deferred remove-all context, for cleaning out a tree that has been removed remotely
struct UG_deferred_remove_ctx {

   struct fskit_core* core;     ///< fskit core
   char* fs_path;               ///< Path to the entry to remove
   fskit_entry_set* children;   ///< The (optional) children to remove (not yet garbage-collected)
};

static int UG_consistency_fetchxattrs_all( struct SG_gateway* gateway, ms_path_t* path_remote, struct ms_client_multi_result* remote_inodes );

/**
 * @brief Helper to asynchronously try to unlink an inode and its children
 */
static int UG_deferred_remove_cb( struct md_wreq* wreq, void* cls ) {

   struct UG_deferred_remove_ctx* ctx = (struct UG_deferred_remove_ctx*)cls;
   struct fskit_detach_ctx* dctx = NULL;
   int rc = 0;

   SG_debug("DEFERRED: remove '%s'\n", ctx->fs_path );

   // remove the children
   if( ctx->children != NULL ) {

      dctx = fskit_detach_ctx_new();
      if( dctx == NULL ) {
         return -ENOMEM;
      }

      rc = fskit_detach_ctx_init( dctx );
      if( rc != 0 ) {
         return rc;
      }

      // proceed to detach
      while( true ) {

         rc = fskit_detach_all_ex( ctx->core, ctx->fs_path, &ctx->children, dctx );
         if( rc == 0 ) {
             break;
         }
         else if( rc == -ENOMEM ) {
             continue;
         }
         else {
             break;
         }
      }

      fskit_detach_ctx_free( dctx );
      SG_safe_free( dctx );
   }

   SG_safe_free( ctx->fs_path );
   fskit_entry_set_free( ctx->children );
   SG_safe_free( ctx );

   return 0;
}


/**
 * @brief Garbage-collect the given inode, and queue it for unlinkage.
 *
 * If the inode is a directory, recursively garbage-collect its children as well, and queue them and their descendents for unlinkage
 * @attention Child must be write-locked
 * @retval 0 Success
 */
int UG_deferred_remove( struct UG_state* state, char const* child_path, struct fskit_entry* child ) {

   struct UG_deferred_remove_ctx* ctx = NULL;
   struct fskit_core* core = UG_state_fs( state );
   struct md_wreq* work = NULL;
   fskit_entry_set* children = NULL;
   int rc = 0;

   // asynchronously unlink it and its children
   ctx = SG_CALLOC( struct UG_deferred_remove_ctx, 1 );
   if( ctx == NULL ) {
       return -ENOMEM;
   }

   work = SG_CALLOC( struct md_wreq, 1 );
   if( work == NULL ) {

       SG_safe_free( ctx );
       return -ENOMEM;
   }

   // set up the deferred unlink request
   ctx->core = core;
   ctx->fs_path = strdup( child_path );

   if( ctx->fs_path == NULL ) {

       SG_safe_free( work );
       SG_safe_free( ctx );
       return -ENOMEM;
   }

   // garbage-collect this child
   rc = fskit_entry_tag_garbage( child, &children );
   if( rc != 0 ) {

       SG_safe_free( ctx );
       SG_safe_free( work );

       SG_error("fskit_entry_garbage_collect('%s') rc = %d\n", child_path, rc );
       return rc;
   }

   ctx->children = children;

   // deferred removal
   md_wreq_init( work, UG_deferred_remove_cb, ctx, 0 );
   md_wq_add( UG_state_wq( state ), work );

   return 0;
}


/**
 * @brief Go fetch the latest version of an inode directly from the MS
 * @param[out] *ent Entry
 * @retval 0 Success, and populate *ent
 * @retval -ENOMEM Out of Memory
 * @retval -EACCES Permission error from the MS
 * @retval -ENOENT The entry doesn't exist on the MS
 * @retval -EREMOTEIO The MS's reply was invalid, or we failed to talk to it
 */
int UG_consistency_inode_download( struct SG_gateway* gateway, uint64_t file_id, struct md_entry* ent ) {

   int rc = 0;
   struct ms_path_ent req;
   struct ms_client* ms = SG_gateway_ms(gateway);
   uint64_t volume_id = ms_client_get_volume_id( ms );

   rc = ms_client_getattr_request( &req, volume_id, file_id, 0, 0, NULL );
   if( rc != 0 ) {
      return rc;
   }

   rc = ms_client_getattr( ms, &req, ent );
   if( rc != 0 ) {
      SG_error("ms_client_getattr(%" PRIX64 ") rc = %d\n", file_id, rc );

      if( rc != -EACCES && rc != -ENOENT ) {
         rc = -EREMOTEIO;
      }

      goto UG_consistency_inode_download_out;
   }

UG_consistency_inode_download_out:
   ms_client_free_path_ent( &req, NULL );
   return rc;
}


/**
 * @brief Download the manifest from one of a list of gateways
 *
 * Try from the cache, and then from each gateway in gateway_ids, in order.
 * @note Does *not* check if the manifest came from a different gateway than the one contacted
 * @param[out] *manifest The manifest
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 * @retval -EINVAL reqdat doesn't refer to a manifest
 * @retval -ENODATA if a manifest could not be fetched (i.e. no gateways online, all manifests obtained were invalid, etc.)
 */
int UG_consistency_manifest_download( struct SG_gateway* gateway, struct SG_request_data* reqdat, uint64_t coordinator_id, uint64_t* gateway_ids, size_t num_gateway_ids, struct SG_manifest* manifest, struct md_download_connection_pool* dlcpool ) {

   int rc = 0;
   SG_messages::Manifest mmsg;
   struct SG_chunk serialized_manifest;
   struct SG_chunk manifest_chunk;
   struct md_download_connection_pool* temp_dlcpool = NULL;
   struct md_download_connection* dlconn = NULL;

   if( !SG_request_is_manifest( reqdat ) ) {
      return -EINVAL;
   }

   // cache?
   rc = SG_gateway_cached_manifest_get_raw( gateway, reqdat, &serialized_manifest );
   if( rc == 0 ) {

      // cache hit.
      // try to verify
      rc = SG_client_parse_manifest( gateway, reqdat, coordinator_id, &serialized_manifest, manifest );
      SG_chunk_free( &serialized_manifest );
      if( rc == 0 ) {
        // cached data is correct
        return 0;
      }
   }

   // will have to download
   if( num_gateway_ids == 0 ) {
      return -ENODATA;
   }

   memset( &serialized_manifest, 0, sizeof(struct SG_chunk));
   memset( &manifest_chunk, 0, sizeof(struct SG_chunk));

   if( dlcpool == NULL ) {
       temp_dlcpool = md_download_connection_pool_new();
       rc = md_download_connection_pool_init( temp_dlcpool );
       if( rc != 0 ) {
           return rc;
       }

       dlcpool = temp_dlcpool;
   }

   for( size_t i = 0; i < num_gateway_ids; i++ ) {

      SG_debug("GET manifest %" PRIX64 ".%" PRId64 "/manifest.%ld.%ld from %" PRIu64 "\n",
            reqdat->file_id, reqdat->file_version, reqdat->manifest_timestamp.tv_sec, reqdat->manifest_timestamp.tv_nsec, gateway_ids[i] );

        // get connection
        dlconn = md_download_connection_pool_get(dlcpool, gateway_ids[i]);
        if( dlconn == NULL ) {
            // delete
             if( temp_dlcpool != NULL ) {
                 md_download_connection_pool_free(temp_dlcpool);
             }
            return -EINVAL;
        }

      rc = SG_client_get_manifest( gateway, reqdat, coordinator_id, gateway_ids[i], manifest, dlconn );
      md_download_connection_pool_make_idle(dlcpool, dlconn);
      if( rc != 0 ) {

         // not from this one
         SG_warn("SG_client_get_manifest(%" PRIX64 ".%" PRId64 "/manifest.%ld.%ld) from %" PRIu64 " by %" PRIu64" rc = %d\n",
                  reqdat->file_id, reqdat->file_version, reqdat->manifest_timestamp.tv_sec, reqdat->manifest_timestamp.tv_nsec, gateway_ids[i], coordinator_id, rc );

         rc = -ENODATA;
         continue;
      }
      else {
         break;
      }
   }

   // delete
    if( temp_dlcpool != NULL ) {
       md_download_connection_pool_free(temp_dlcpool);
    }

   return rc;
}


int UG_consistency_manifest_ensure_fresh( struct SG_gateway* gateway, char const* fs_path ) {
    return UG_consistency_manifest_ensure_fresh2(gateway, fs_path, NULL);
}

/**
 * @brief Verify that a manifest is fresh.  Download and merge the latest manifest data for the referred inode if not.
 *
 * Local dirty blocks that were overwritten will be dropped and evicted on merge.
 * @note Entry at the end of fs_path should *NOT* be locked
 * @note The caller should refresh the inode first, since the manifest timestamp may have changed on the MS
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 * @retval -ENODATA Could not fetch a manifest, but needed to
 */
int UG_consistency_manifest_ensure_fresh2( struct SG_gateway* gateway, char const* fs_path, struct md_download_connection_pool* dlcpool ) {

   int rc = 0;
   struct SG_manifest new_manifest;
   struct SG_request_data reqdat;

   uint64_t* gateway_ids_buf = NULL;
   size_t num_gateway_ids = 0;

   int64_t manifest_mtime_sec = 0;
   int32_t manifest_mtime_nsec = 0;

   struct fskit_entry* fent = NULL;
   struct UG_inode* inode = NULL;

   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );

   struct timespec now;
   int32_t max_read_freshness = 0;
   struct timespec manifest_refresh_mtime;

   uint64_t file_id = 0;
   int64_t file_version = 0;
   uint64_t coordinator_id = 0;
   bool local_coordinator = false;

   memset( &now, 0, sizeof(struct timespec) );
   memset( &manifest_refresh_mtime, 0, sizeof(struct timespec) );

   // keep around...
   fent = fskit_entry_ref( fs, fs_path, &rc );
   if( rc != 0 ) {

      SG_error("BUG: fskit_entry_ref(%s) rc = %d\n", fs_path, rc );
      exit(1);
      return rc;
   }

   fskit_entry_wlock( fent );

   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );

   manifest_refresh_mtime = UG_inode_manifest_refresh_time( inode );
   file_id = UG_inode_file_id( inode );
   file_version = UG_inode_file_version( inode );
   coordinator_id = UG_inode_coordinator_id( inode );
   max_read_freshness = UG_inode_max_read_freshness( inode );

   // TODO: test this-we update manifest modtime between writes, and refresh manifest as well
   SG_manifest_get_modtime( UG_inode_manifest( inode ), &manifest_mtime_sec, &manifest_mtime_nsec );

   // are we the coordinator?
   if( SG_gateway_id( gateway ) == SG_manifest_get_coordinator( UG_inode_manifest( inode ) ) ) {

      local_coordinator = true;
   }

   // if we're the coordinator and we didn't explicitly mark the manifest as stale, then it's fresh
   if( !SG_manifest_is_stale( UG_inode_manifest( inode ) ) && local_coordinator ) {

      // we're the coordinator--we already have the freshest version
      SG_debug("Manifest %" PRIX64 ".%" PRId64 ".%d is locally-coordinated and not stale\n", file_id, manifest_mtime_sec, manifest_mtime_nsec );
      fskit_entry_unlock( fent );
      fskit_entry_unref( fs, fs_path, fent );
      return 0;
   }

   SG_debug("Reload manifest %" PRIX64 "/manifest.%" PRId64 ".%d (coordinator? %d)\n", file_id, manifest_mtime_sec, manifest_mtime_nsec, local_coordinator );

   rc = clock_gettime( CLOCK_REALTIME, &now );
   if( rc != 0 ) {

      rc = -errno;
      SG_error("clock_gettime rc = %d\n", rc );

      fskit_entry_unlock( fent );
      fskit_entry_unref( fs, fs_path, fent );
      return rc;
   }

   // is the manifest stale?
   if( !SG_manifest_is_stale( UG_inode_manifest( inode ) ) && md_timespec_diff_ms( &now, &manifest_refresh_mtime ) <= max_read_freshness ) {

      // still fresh
      SG_debug("Manifest %" PRIX64 "/manifest.%" PRId64 ".%d is still fresh\n", file_id, manifest_mtime_sec, manifest_mtime_nsec );
      fskit_entry_unlock( fent );
      fskit_entry_unref( fs, fs_path, fent );
      return 0;
   }

   // manifest is stale--must refresh.
   // get list of gateways to try
   rc = UG_read_download_gateway_list( gateway, coordinator_id, &gateway_ids_buf, &num_gateway_ids );
   if( rc != 0 ) {

      fskit_entry_unlock( fent );
      fskit_entry_unref( fs, fs_path, fent );
      return rc;
   }

   if( num_gateway_ids == 0 ) {

      // no gateways
      SG_error("%s", "No replica gateways exist; cannot fetch manifest\n");
      SG_safe_free( gateway_ids_buf );
      fskit_entry_unlock( fent );
      fskit_entry_unref( fs, fs_path, fent );
      return -ENODATA;
   }
   else {
      SG_debug("Try downloading %" PRIX64 ".%" PRId64 "/manifest.%ld.%d from:\n", file_id, file_version, manifest_mtime_sec, manifest_mtime_nsec );
      for( uint64_t i = 0; i < num_gateway_ids; i++ ) {
         SG_debug("   %" PRIu64 "\n", gateway_ids_buf[i] );
      }
   }

   // set up a request
   rc = SG_request_data_init_manifest( gateway, fs_path, file_id, file_version, manifest_mtime_sec, manifest_mtime_nsec, &reqdat );
   if( rc != 0 ) {

      SG_safe_free( gateway_ids_buf );
      fskit_entry_unlock( fent );
      fskit_entry_unref( fs, fs_path, fent );
      return rc;
   }

   // get the manifest
   rc = UG_consistency_manifest_download( gateway, &reqdat, coordinator_id, gateway_ids_buf, num_gateway_ids, &new_manifest, dlcpool );
   SG_safe_free( gateway_ids_buf );

   if( rc != 0 ) {

      SG_error("UG_consistency_manifest_download(%" PRIX64 ".%" PRId64 "/manifest.%ld.%ld) rc = %d\n",
               reqdat.file_id, reqdat.file_version, reqdat.manifest_timestamp.tv_sec, reqdat.manifest_timestamp.tv_nsec, rc );

      SG_request_data_free( &reqdat );
      fskit_entry_unlock( fent );
      fskit_entry_unref( fs, fs_path, fent );
      return rc;
   }

   // merge in new blocks (but keep locally-dirty ones)
   rc = UG_inode_manifest_merge_blocks( gateway, inode, &new_manifest );
   if( rc == 0 ) {

      // restore modtime, version, coordinator, size
      SG_manifest_set_modtime( UG_inode_manifest( inode ), SG_manifest_get_modtime_sec( &new_manifest ), SG_manifest_get_modtime_nsec( &new_manifest ) );
      SG_manifest_set_coordinator_id( UG_inode_manifest( inode ), SG_manifest_get_coordinator( &new_manifest ) );

      if( SG_manifest_get_file_version( UG_inode_manifest( inode ) ) < SG_manifest_get_file_version( &new_manifest ) ) {
         // version advanced.  take remote's size
         UG_inode_set_size( inode, SG_manifest_get_file_size( &new_manifest ) );
      }
      else {
         UG_inode_set_size( inode, MAX( SG_manifest_get_file_size( &new_manifest ), UG_inode_size( inode ) ) );
      }

      SG_manifest_set_file_version( UG_inode_manifest( inode ), SG_manifest_get_file_version( &new_manifest ) );

      // update refresh time
      rc = clock_gettime( CLOCK_REALTIME, &now );
      if( rc != 0 ) {

         rc = -errno;
         SG_error("clock_gettime rc = %d\n", rc );

         // mask--the worst that'll happen is we refresh too much
         rc = 0;
      }
      else {

         // advance refresh time
         UG_inode_set_manifest_refresh_time_now( inode );
      }
   }
   else {

      SG_error("UG_inode_manifest_merge_blocks( %" PRIX64 ".%" PRId64 "/manifest.%ld.%ld ) rc = %d\n",
                reqdat.file_id, reqdat.file_version, reqdat.manifest_timestamp.tv_sec, reqdat.manifest_timestamp.tv_nsec, rc );

   }

   SG_manifest_set_stale( UG_inode_manifest( inode ), false );

   fskit_entry_unlock( fent );
   fskit_entry_unref( fs, fs_path, fent );
   SG_manifest_free( &new_manifest );
   SG_request_data_free( &reqdat );

   return rc;
}


/**
 * @brief Replace one fskit_entry with another.
 *
 * Deferred-delete the old fent.
 * @attention fent must be write-locked
 * @retval 0 Success
 * @retval -errno Failure
 * @retval EAGAIN Successfully attached, but failed to remove the old fent
 */
static int UG_consistency_fskit_entry_replace( struct SG_gateway* gateway, char const* fs_path, struct fskit_entry* parent, struct fskit_entry* fent, struct fskit_entry* new_fent ) {

   int rc = 0;

   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );

   char* basename = fskit_basename( fs_path, NULL );
   if( basename == NULL ) {
       return -ENOMEM;
   }

   struct UG_inode* inode = (struct UG_inode*)fskit_entry_get_user_data( fent );

   struct md_syndicate_cache* cache = SG_gateway_cache( gateway );

   // blow away this file/directory and its children
   rc = UG_deferred_remove( ug, fs_path, fent );
   if( rc != 0 ) {

      SG_error("UG_deferred_remove( '%s' ) rc = %d\n", fs_path, rc );

      fskit_entry_destroy( fs, new_fent, false );
      SG_safe_free( new_fent );
      SG_safe_free( basename );

      return rc;
   }

   // put the new one in place
   rc = fskit_entry_attach_lowlevel( parent, new_fent, basename );
   SG_safe_free( basename );

   if( rc != 0 ) {

      SG_error("fskit_entry_attach_lowlevel( '%s' ) rc = %d\n", fs_path, rc );

      // NOTE: don't try to reinsert--the old one was gone either way
      fskit_entry_destroy( fs, new_fent, false );
      SG_safe_free( new_fent );

      return rc;
   }

   // blow away the inode's cached data
   // (NOTE: don't care if this fails--it'll get reaped eventually)
   md_cache_evict_file( cache, fskit_entry_get_file_id( fent ), UG_inode_file_version( inode ), 0 );

   UG_inode_free( inode );
   inode = NULL;

   if( rc != 0 ) {

      SG_error("UG_deferred_remove('%s') rc = %d\n", fs_path, rc );
   }

   return rc;
}


/**
 * @brief Reload a single inode's metadata.
 *
 * If the types don't match, the inode (and its children) will be dropped and a new inode with the new type will be created in its place.
 * If the versions don't match, then the inode will be reversioned
 * For regular files, if the size changed, then the inode will be truncated (i.e. evicting blocks if the size shrank)
 * If the names don't match, the name will be changed.
 * If this is a regular file, and we're still the coordinator and the version has not changed, then no reload will take place (since we already have the latest information).
 * @attention fent must be write-locked
 * @attention parent must be write-locked
 * @note fent might be replaced--don't access it after calling this method.
 * @retval 0 Success
 * @retval 1 fent got replaced
 * @retval -ENOMEM Out of Memory
 * @retval -errno Error
 */
int UG_consistency_inode_reload( struct SG_gateway* gateway, char const* fs_path, struct fskit_entry* parent, struct fskit_entry* fent, char const* fent_name, struct md_entry* inode_data ) {

   int rc = 0;
   struct fskit_entry* new_fent = NULL;

   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );

   struct UG_inode* inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   struct md_syndicate_cache* cache = SG_gateway_cache( gateway );

   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t block_size = ms_client_get_volume_blocksize( ms );

   int64_t old_version = UG_inode_file_version( inode );

   // types don't match, and version advanced?
   if( !UG_inode_export_match_type( inode, inode_data ) && inode_data->version > old_version ) {

      SG_debug("%" PRIX64 ": old type = %d, new type = %d\n", inode_data->file_id, fskit_entry_get_type( UG_inode_fskit_entry( inode ) ), inode_data->type );

      // make a new fskit entry for this
      new_fent = fskit_entry_new();
      if( new_fent == NULL ) {
         return -ENOMEM;
      }

      // build the new fent
      rc = UG_inode_fskit_entry_init( fs, new_fent, parent, inode_data );
      if( rc != 0 ) {

         SG_error("UG_inode_fskit_entry_init( '%s' (%" PRIX64 ") ) rc = %d\n", inode_data->name, inode_data->file_id, rc );

         // OOM
         fskit_entry_destroy( fs, new_fent, false );
         SG_safe_free( new_fent );
         return rc;
      }

      // replace in parent
      rc = UG_consistency_fskit_entry_replace( gateway, fs_path, parent, fent, new_fent );
      if( rc != 0 ) {

         SG_error("UG_consistency_fskit_entry_replace( '%s' ) rc = %d\n", fs_path, rc );

         if( rc < 0 ) {

            // failed to attach
            struct UG_inode* new_inode = (struct UG_inode*)fskit_entry_get_user_data( new_fent );

            fskit_entry_destroy( fs, new_fent, false );
            SG_safe_free( new_fent );

            UG_inode_free( new_inode );
            SG_safe_free( new_inode );

            return rc;
         }
         else {

            // failed to garbage-collect
            SG_error("LEAK: failed to garbage-collect old inode for '%s'.  Consider filing a bug report!\n", fs_path);
            rc = 0;
         }
      }

      // if this is a file, it's manifest is stale--we'll want to reload the block information as well
      if( fskit_entry_get_type( new_fent ) == FSKIT_ENTRY_TYPE_FILE ) {

         SG_manifest_set_stale( UG_inode_manifest( inode ), true );
         SG_debug("%" PRIX64 ": mark manifest stale\n", UG_inode_file_id( inode ) );
      }

      // replaced!
      // nothing more to do--the new inode has the right version, name, and size
      UG_inode_set_refresh_time_now( inode );
      return 1;
   }

   // versions don't match?
   if( !UG_inode_export_match_version( inode, inode_data ) ) {

      // reversion--both metadata, and cached data, and staged data
      SG_debug("%" PRIX64 ": old version = %" PRId64 ", new version = %" PRId64 "\n", inode_data->file_id, UG_inode_file_version( inode ), inode_data->version );

      // NOTE: don't really care if cache reversioning fails--it'll get reaped eventually
      md_cache_reversion_file( cache, inode_data->file_id, UG_inode_file_version( inode ), inode_data->version, 0 );
      SG_manifest_set_file_version( UG_inode_manifest( inode ), inode_data->version );
   }
   else if( fskit_entry_get_type(fent) == FSKIT_ENTRY_TYPE_FILE ) {

      // if version matches and we're the coordinator, and this is a file, then no further action is necessary.
      if( SG_gateway_id( gateway ) == UG_inode_coordinator_id( inode ) ) {

         // our copy is fresh if it's a file...
         SG_debug("%" PRIX64 " is coordinated locally\n", inode_data->file_id );
         UG_inode_set_refresh_time_now( inode );
         UG_inode_set_read_stale( inode, false );

         return 0;
      }
   }

   // file sizes don't match, and version advanced?
   if( fskit_entry_get_type( fent ) == FSKIT_ENTRY_TYPE_FILE && !UG_inode_export_match_size( inode, inode_data ) ) {

      // need to expand/truncate inode
      off_t size = fskit_entry_get_size( UG_inode_fskit_entry( inode ) );
      off_t new_size = inode_data->size;

      SG_debug("%" PRIX64 ": old size = %jd, new size = %jd\n", inode_data->file_id, size, new_size );

      // only shrink if version changed
      if( size > new_size && inode_data->version > old_version ) {

         // shrunk
         SG_debug("%" PRIX64 ": shrinking to %jd\n", inode_data->file_id, new_size);

         uint64_t max_block_id = (new_size / block_size);

         for( SG_manifest_block_iterator itr = SG_manifest_block_iterator_begin( UG_inode_manifest( inode ) ); itr != SG_manifest_block_iterator_end( UG_inode_manifest( inode ) ); itr++ ) {

            if( SG_manifest_block_iterator_id( itr ) <= max_block_id ) {
               continue;
            }

            // NOTE: don't really care if these fail; they'll get reaped eventually
            md_cache_evict_block_async( cache, UG_inode_file_id( inode ), UG_inode_file_version( inode ), SG_manifest_block_iterator_id( itr ), (SG_manifest_block_iterator_block( itr ))->block_version );
         }

         SG_manifest_truncate( UG_inode_manifest( inode ), max_block_id );
      }
      else if( size <= new_size ) {

         // grew
         SG_manifest_set_stale( UG_inode_manifest( inode ), true );
      }

      SG_manifest_set_size( UG_inode_manifest( inode ), new_size );
   }

   // names don't match, and version advanced?
   if( UG_inode_export_match_name( inode, inode_data ) <= 0 && inode_data->version > old_version ) {

      // inode got renamed
      SG_debug("%" PRIX64 ": old name = '%s', new name = '%s'; do rename\n", inode_data->file_id, UG_inode_name_ref( inode ), inode_data->name );

      // change inode name
      rc = UG_inode_set_name( inode, inode_data->name );
      if( rc != 0 ) {

         // OOM
         return rc;
      }

      rc = fskit_entry_rename_in_directory( parent, fent, fent_name, inode_data->name );
      if( rc != 0 ) {

         // OOM
         SG_error("fskit_entry_rename_in_directory( '%s' ) rc = %d\n", inode_data->name, rc );
         return rc;
      }
   }

   // manifest timestamps don't match, and we don't coordinate this file?
   if( fskit_entry_get_type( fent ) == FSKIT_ENTRY_TYPE_FILE && UG_inode_coordinator_id( inode ) != SG_gateway_id( gateway ) &&
      (inode_data->manifest_mtime_sec != SG_manifest_get_modtime_sec( UG_inode_manifest( inode ) ) || inode_data->manifest_mtime_nsec != SG_manifest_get_modtime_nsec( UG_inode_manifest( inode ) )) ) {

      SG_debug("%" PRIX64 ": old manifest timestamp = %" PRId64 ".%d, new manifest timestamp = %" PRId64 ".%d\n", inode_data->file_id,
            SG_manifest_get_modtime_sec( UG_inode_manifest( inode ) ), SG_manifest_get_modtime_nsec( UG_inode_manifest( inode )),
            inode_data->manifest_mtime_sec, inode_data->manifest_mtime_nsec );

      SG_manifest_set_stale( UG_inode_manifest( inode ), true );
   }

   // change of coordinator, and version advanced?
   if( UG_inode_coordinator_id( inode ) == SG_gateway_id( gateway ) && inode_data->coordinator != SG_gateway_id( gateway ) && inode_data->version > old_version ) {

      // uncache xattrs--we're not the authoritative source any longer
      SG_debug("%" PRIX64 ": old coordinator = %" PRIu64 ", new coordinator = %" PRIu64 "\n", inode_data->file_id, SG_gateway_id( gateway ), inode_data->coordinator );

      fskit_fremovexattr_all( fs, fent );
   }

   // reload everything else
   SG_debug("Import '%s' (%" PRIX64 ")\n", inode_data->name, inode_data->file_id );
   rc = UG_inode_import( inode, inode_data );
   if( rc == 0 ) {

      // reloaded!
      // no longer stale
      UG_inode_set_read_stale( inode, false );
      UG_inode_set_refresh_time_now( inode );

      // only update the manifest refresh time if we're NOT the coordinator
      if( fskit_entry_get_type( fent ) == FSKIT_ENTRY_TYPE_FILE && UG_inode_coordinator_id( inode ) != SG_gateway_id( gateway ) ) {

         SG_manifest_set_modtime( UG_inode_manifest( inode ), inode_data->manifest_mtime_sec, inode_data->manifest_mtime_nsec );
      }
   }
   else {

      SG_error("UG_inode_import( '%s' (%" PRIX64 ") ) rc = %d\n", inode_data->name, inode_data->file_id, rc );
   }

   return rc;
}


/**
 * @brief Free a graft
 *
 * Free a graft, a chain of fskit_entry structures built from UG_consistency_fskit_path_graft_build.
 * Do not detach the inodes, we don't want to run the unlink routes.
 * Destroy graft_parent and all of its children.
 * @see UG_consistency_fskit_path_graft_build
 * @return 0
 */
static int UG_consistency_fskit_path_graft_free( struct fskit_core* fs, struct fskit_entry* graft_parent, struct md_entry* path_data, size_t path_len ) {

   if( graft_parent == NULL ) {
      return 0;
   }

   struct fskit_entry* graft_child = NULL;
   int i = 0;

   while( (unsigned)i < path_len ) {

      // search graft parent
      graft_child = fskit_dir_find_by_name( graft_parent, path_data[i].name );
      if( graft_child == NULL ) {

         // done
         break;
      }

      // destroy graft parent
      fskit_entry_destroy( fs, graft_parent, false );
      SG_safe_free( graft_parent );

      graft_parent = graft_child;

      i++;
   }

   return 0;
}


/**
 * @brief Construct a graft--a chain of fskit_entry structures--from an ordered list of inode metadata.
 *
 * Construct a graft--a chain of fskit_entry structures--from an ordered list of inode metadata.
 * Do not attach it to fskit; just build it up.
 * Remote_path->at(i) should match path_data[i].
 * If remote_path->at(i) is bound to anything, it should be bound to a malloc'ed fskit_xattr_set that contains the node's xattrs (fetched if this gateway is the coordinator)
 * @note Don't destroy path_data just yet--keep it around so we know how to look up and free the graft later on, if need be
 * @param[out] *graft_root The root of the graft, it can have no parent (i.e. a NULL parent for "..")
 * @retval 0 Success
 * @retval -EINVAL Invalid data (i.e. the path_data contains a non-leaf directory, etc.)
 * @retval -ENOMEM Out of Memory
 */
static int UG_consistency_fskit_path_graft_build( struct SG_gateway* gateway, ms_path_t* remote_path, struct md_entry* path_data, size_t path_len, struct fskit_entry** graft_root ) {

   int rc = 0;

   struct fskit_entry* graft_parent = NULL;
   struct fskit_entry* graft_child = NULL;

   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );
   struct UG_inode* inode = NULL;

   fskit_xattr_set* xattrs = NULL;
   fskit_xattr_set* old_xattrs = NULL;

   // sanity check--all path_data elements except the leaf must be directories
   for( int i = 0; i < (int)path_len - 1; i++ ) {

      if( path_data[i].type != MD_ENTRY_DIR ) {

         return -EINVAL;
      }
   }

   for( size_t i = 0; i < path_len; i++ ) {

      // next child
      graft_child = fskit_entry_new();
      if( graft_child == NULL ) {

         rc = -ENOMEM;

         if( *graft_root != NULL ) {
            UG_consistency_fskit_path_graft_free( fs, *graft_root, path_data, path_len );
         }

         return rc;
      }

      SG_debug("Graft %s %" PRIX64 "\n", (path_data[i].type == MD_ENTRY_DIR ? "directory" : "file"), path_data[i].file_id );

      // build the inode
      rc = UG_inode_fskit_entry_init( fs, graft_child, graft_parent, &path_data[i] );
      if( rc != 0 ) {

         SG_error("UG_inode_fskit_entry_init( %" PRIX64 " (%s) ) rc = %d\n", path_data[i].file_id, path_data[i].name, rc );

         if( *graft_root != NULL ) {
            UG_consistency_fskit_path_graft_free( fs, *graft_root, path_data, path_len );
         }

         return rc;
      }

      inode = (struct UG_inode*)fskit_entry_get_user_data( graft_child );

      if( path_data[i].type == MD_ENTRY_FILE ) {

          // file manifest should be stale, since we only have metadata
          SG_manifest_set_stale( UG_inode_manifest( inode ), true );
          SG_debug("%" PRIX64 ": mark manifest stale\n", UG_inode_file_id( inode ) );
      }
      else {

          // directory children should be stale, since we only have metadata
          struct timespec zero;
          memset( &zero, 0, sizeof(struct timespec) );

          UG_inode_set_children_refresh_time( inode, &zero );
      }

      // metadata is fresh
      UG_inode_set_read_stale( inode, false );
      UG_inode_set_refresh_time_now( inode );

      // transfer xattrs
      xattrs = (fskit_xattr_set*)ms_client_path_ent_get_cls( &remote_path->at(i) );
      if( xattrs != NULL ) {

          old_xattrs = fskit_entry_swap_xattrs( graft_child, xattrs );

          if( old_xattrs != NULL ) {
              fskit_xattr_set_free( old_xattrs );
              old_xattrs = NULL;
          }
      }

      ms_client_path_ent_set_cls( &remote_path->at(i), NULL );

      // insert the inode into its parent (except for the root, which we'll do later)
      if( graft_parent != NULL ) {

         rc = fskit_entry_attach_lowlevel( graft_parent, graft_child, path_data[i].name );
         if( rc != 0 ) {

            SG_error("fskit_entry_attach_lowlevel( %" PRIX64 " --> %" PRIX64 " (%s) ) rc = %d\n", fskit_entry_get_file_id( graft_parent ), path_data[i].file_id, path_data[i].name, rc );

            fskit_entry_destroy( fs, graft_child, false );
            UG_consistency_fskit_path_graft_free( fs, *graft_root, path_data, path_len );
            return rc;
         }
      }

      // set *graft_root if this is the first
      if( i == 0 ) {

         *graft_root = graft_child;
      }

      // next entry
      graft_parent = graft_child;
      graft_child = NULL;
   }

   // success!
   return 0;
}


/**
 * @brief Attach a graft to an fskit_entry, based on its parent's ID and the path that the graft was generated from.
 * @retval 0 Success
 * @retval -ENOENT The parent could not be found
 * @retval -EEXIST There is an existing entry with graft_root's name
 * @retval -ENOTDIR The parent is not a directory
 * @retval -ENOMEM Out of Memory
 */
static int UG_consistency_fskit_path_graft_attach( struct SG_gateway* gateway, char const* fs_path, uint64_t parent_id, char const* graft_root_name, struct fskit_entry* graft_root ) {

   int rc = 0;

   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );

   bool attached = false;

   struct fskit_path_iterator* itr = NULL;

   if( graft_root == NULL ) {
      return -EINVAL;
   }

   // find the attachment point
   itr = fskit_path_begin( fs, fs_path, true );
   if( itr == NULL ) {

      return -ENOMEM;
   }

   for( ; !fskit_path_end( itr ); fskit_path_next( itr ) ) {

      // current entry
      struct fskit_entry* cur = fskit_path_iterator_entry( itr );
      struct fskit_entry* collision = NULL;

      if( fskit_entry_get_file_id( cur ) == parent_id ) {

         // has to be a dir
         if( fskit_entry_get_type( cur ) != FSKIT_ENTRY_TYPE_DIR ) {

            rc = -ENOTDIR;
            break;
         }

         // graft point exists already?
         collision = fskit_dir_find_by_name( cur, graft_root_name );
         if( collision != NULL ) {

            // exists
            char* tmppath = fskit_path_iterator_path( itr );
            SG_error("Directory '%s' has child '%s' already!\n", tmppath, graft_root_name );
            SG_safe_free( tmppath );

            rc = -EEXIST;
            break;
         }

         // attach!
         rc = fskit_entry_attach_lowlevel( cur, graft_root, graft_root_name );

         if( rc == 0 ) {
            attached = true;
         }

         break;
      }
   }

   // done with this iterator
   fskit_path_iterator_release( itr );

   if( rc == 0 && !attached ) {

      // that's odd--no point to attach to
      rc = -ENOENT;
   }

   return rc;
}


/**
 * @brief Free a path's associated path contexts, and unref its entries
 * @retval 0 Success
 */
static int UG_consistency_path_free( struct fskit_core* core, ms_path_t* path ) {

    // unref all
    for( unsigned int i = 0; i < path->size(); i++ ) {

        struct UG_path_ent_ctx* ent_ctx = (struct UG_path_ent_ctx*)ms_client_path_ent_get_cls( &path->at(i) );
        if( ent_ctx == NULL ) {
            continue;
        }

        fskit_entry_unref( core, ent_ctx->fs_path, ent_ctx->fent );
        SG_safe_free( ent_ctx->fs_path );
        SG_safe_free( ent_ctx );

        ms_client_path_ent_set_cls( &path->at(i), NULL );
    }

    ms_client_free_path( path, NULL );
    return 0;
}


/**
 * @brief Build up an ms_path_t of locally-cached but stale fskit entries.
 *
 * For each entry in path_local, bind the associated the fskit entry to the path.
 * @note path_local is not guaranteed to be a contiguous path, we will skip fresh entries
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 */
static int UG_consistency_path_find_local_stale( struct SG_gateway* gateway, char const* fs_path, struct timespec* refresh_begin, ms_path_t* path_local ) {

   int rc = 0;

   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );

   struct fskit_path_iterator* itr = NULL;

   itr = fskit_path_begin( fs, fs_path, true );
   if( itr == NULL ) {

      return -ENOMEM;
   }

   for( ; !fskit_path_end( itr ); fskit_path_next( itr ) ) {

      struct fskit_entry* cur = fskit_path_iterator_entry( itr );
      struct UG_inode* inode = (struct UG_inode*)fskit_entry_get_user_data( cur );

      struct ms_path_ent path_ent;
      struct UG_path_ent_ctx* path_ctx = NULL;  // remember the entries we reference

      // is this inode stale?
      if( !UG_inode_is_read_stale( inode, refresh_begin ) ) {

         char* name = fskit_path_iterator_name( itr );
         SG_debug("fresh: '%s' /%" PRIu64 "/%" PRIX64 ".%" PRId64 ", %" PRId64 "\n", name, UG_inode_volume_id( inode ), UG_inode_file_id( inode ), UG_inode_file_version( inode ), UG_inode_write_nonce( inode ) );
         SG_safe_free( name );

         continue;
      }
      else {

         char* name = fskit_path_iterator_name( itr );
         struct timespec refresh_time = UG_inode_refresh_time( inode );

         SG_debug("stale: '%s' /%" PRIu64 "/%" PRIX64 ".%" PRId64 ", %" PRId64 " (mtime: %" PRId64 ".%ld, refresh_begin: %" PRId64 ".%ld, diff = %" PRId64 ", max = %d, is_stale = %d)\n",
                  name, UG_inode_volume_id( inode ), UG_inode_file_id( inode ), UG_inode_file_version( inode ), UG_inode_write_nonce( inode ),
                  refresh_time.tv_sec, refresh_time.tv_nsec, refresh_begin->tv_sec, refresh_begin->tv_nsec, md_timespec_diff_ms( refresh_begin, &refresh_time ),
                  UG_inode_max_read_freshness( inode ), UG_inode_is_read_stale( inode, NULL ) );

         SG_safe_free( name );
      }

      path_ctx = SG_CALLOC( struct UG_path_ent_ctx, 1 );
      if( path_ctx == NULL ) {
         rc = -ENOMEM;
         break;
      }

      char* cur_path = fskit_path_iterator_path( itr );
      if( cur_path == NULL ) {
         SG_safe_free( path_ctx );
         rc = -ENOMEM;
         break;
      }

      // keep this fent around
      fskit_entry_ref_entry( cur );

      path_ctx->fent = cur;
      path_ctx->fs_path = cur_path;

      rc = ms_client_getattr_request( &path_ent, UG_inode_volume_id( inode ), UG_inode_file_id( inode ), UG_inode_file_version( inode ), UG_inode_write_nonce( inode ), path_ctx );
      if( rc != 0 ) {

         // OOM
         SG_safe_free( path_ctx );
         SG_safe_free( cur_path );
         break;
      }

      try {

         path_local->push_back( path_ent );
      }
      catch( bad_alloc& ba ) {

         rc = -ENOMEM;
         SG_safe_free( path_ctx );
         SG_safe_free( cur_path );
         break;
      }
   }

   // done with this iterator
   fskit_path_iterator_release( itr );

   if( rc != 0 ) {

       // unref all
       UG_consistency_path_free( fs, path_local );
   }

   return rc;
}


/**
 * @brief Reload cached stale metadata entries from inode data.
 *
 * Ff the MS indicates that an inode got removed remotely, then delete the cached inode locally and all of its children (if it has any) and terminate.
 * @note inode_data must be in the same order as the inodes that appear in fskit.
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 * @retval -EINVAL The order of inode_data is out-of-whack with fskit
 */
static int UG_consistency_path_stale_reload( struct SG_gateway* gateway, char const* fs_path, ms_path_t* path_stale, struct md_entry* inode_data, size_t num_inodes ) {

   int rc = 0;

   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );
   char* name = NULL;           // inode name in fskit
   uint64_t file_id = 0;        // inode ID
   size_t inode_i = 0;          // indexes inode_data
   struct UG_inode* inode = NULL;
   char* cur_path = NULL;
   bool skip = false;

   if( num_inodes == 0 ) {
      return 0;
   }

   struct fskit_path_iterator* itr = NULL;

   // reload each stale inode
   itr = fskit_path_begin( fs, fs_path, true );
   if( itr == NULL ) {

      return -ENOMEM;
   }

   for( ; !fskit_path_end( itr ); fskit_path_next( itr ) ) {

      struct fskit_entry* cur = fskit_path_iterator_entry( itr );
      char* cur_name = fskit_path_iterator_name( itr );
      struct fskit_entry* parent = fskit_path_iterator_entry_parent( itr );

      file_id = fskit_entry_get_file_id( cur );
      inode = (struct UG_inode*)fskit_entry_get_user_data( cur );

      // if not stale, then skip
      skip = true;
      for( unsigned int j = 0; j < path_stale->size(); j++ ) {
          if( path_stale->at(j).file_id == file_id ) {
              skip = false;
              break;
          }
      }

      if( skip ) {
          // this inode is fresh
          SG_safe_free( cur_name );
          continue;
      }

      if( inode_i >= num_inodes ) {

         SG_error("overflow: counted %zu inodes\n", inode_i );
         SG_safe_free( cur_name );
         rc = -EINVAL;
         break;
      }

      // next datum
      struct md_entry* inode_datum = &inode_data[ inode_i ];

      // is this the fskit entry to reload?
      if( file_id != inode_datum->file_id ) {

         // nope--this one's fresh. dig deeper
         SG_debug("skip: '%s' (%" PRIX64 ")\n", cur_name, file_id );
         SG_safe_free( cur_name );

         continue;
      }

      SG_debug("Consider %" PRIX64 ".%" PRId64 ".%" PRId64 "\n",
               inode_data[inode_i].file_id, inode_data[inode_i].version, inode_data[inode_i].write_nonce );

      // is there any change to reload?
      if( inode_datum->error == MS_LISTING_NOCHANGE ) {

         // nope--nothing to do
         inode_i++;

         // mark fresh
         UG_inode_set_read_stale( inode, false );
         UG_inode_set_refresh_time_now( inode );

         /////////////////////////////////////
         SG_debug("No Change: '%s' (%" PRIX64 ")\n", cur_name, file_id );
         /////////////////////////////////////

         SG_safe_free( cur_name );
         continue;
      }


      // does this inode exist on the MS?
      if( inode_datum->error == MS_LISTING_NONE ) {

         SG_debug("Remove: '%s' (%" PRIu64 ")\n", inode_datum->name, inode_datum->file_id );
         SG_safe_free( cur_name );

         // nope--this inode and everything beneath it got unlinked remotely
         // blow them all away locally
         char* path_stump = fskit_path_iterator_path( itr );
         if( path_stump == NULL ) {

            rc = -ENOMEM;
            break;
         }

         rc = UG_deferred_remove( ug, path_stump, cur );
         if( rc != 0 ) {

            SG_error( "UG_deferred_remove('%s') rc = %d\n", path_stump, rc );
         }

         SG_safe_free( path_stump );

         // done iterating
         break;
      }

      // name of this inode, in case it gets blown away?
      name = SG_strdup_or_null( inode_datum->name );
      if( name == NULL ) {

         rc = -ENOMEM;
         SG_safe_free( cur_name );

         break;
      }

      // reload
      cur_path = fskit_path_iterator_path( itr );
      if( cur_path == NULL ) {
         rc = -ENOMEM;
         SG_safe_free( cur_name );
         break;
      }

      SG_debug("Reload: '%s' (%" PRIu64 ", size=%" PRIu64 ")\n", cur_path, inode_datum->file_id, (uint64_t)fskit_entry_get_size(cur) );
      rc = UG_consistency_inode_reload( gateway, cur_path, parent, cur, cur_name, inode_datum );
      SG_safe_free( cur_name );

      if( rc < 0 ) {

         SG_error("UG_consistency_inode_reload( '%s' (at %" PRIX64 " (%s))) rc = %d\n", cur_path, fskit_entry_get_file_id( cur ), name, rc );

         SG_safe_free( name );
         SG_safe_free( cur_path );

         break;
      }

      SG_safe_free( cur_path );

      if( rc > 0 ) {

         // cur got replaced.
         // reload it
         cur = fskit_dir_find_by_name( parent, name );

         if( cur == NULL ) {

            // not found--this and all inodes beneath us are gone
            SG_safe_free( name );

            rc = -ENOENT;
            break;
         }
      }

      SG_safe_free( name );

      // success!  next entry
      inode_i++;
   }

   // done iterating
   fskit_path_iterator_release( itr );

   return rc;
}


/**
 * @brief Build up a path of download requests for remote entries
 * @param[out] *path_remote Fill with remote inode data (could be empty)
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 */
static int UG_consistency_path_find_remote( struct SG_gateway* gateway, char const* fs_path, ms_path_t* path_remote ) {

   int rc = 0;

   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );

   struct UG_inode* inode = NULL;

   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t volume_id = ms_client_get_volume_id( ms );

   struct fskit_path_iterator* itr = NULL;

   struct ms_path_ent deepest_ent;
   uint64_t deepest_ent_parent_id = 0;
   uint64_t deepest_ent_file_id = 0;
   char* remote_head = NULL;
   int itr_error = 0;
   int depth = 0;

   char** names = NULL;

   // in order to build up the contents of path_remote, we need
   // the first entry of path_remote to have information known
   // to the deepest known fskit entry (volume_id, file_id, name, parent_id).
   // the remaining entries only need names and volume_id.
   // find this parent, and populate it.
   // This means, find the entry at the end of the locally-cached path, but keep track of how deep it is too.
   itr = fskit_path_begin( fs, fs_path, false );
   if( itr == NULL ) {
      return -ENOMEM;
   }

   for( ; !fskit_path_end( itr ); fskit_path_next( itr ) ) {

      struct fskit_entry* cur = fskit_path_iterator_entry( itr );

      inode = (struct UG_inode*)fskit_entry_get_user_data( cur );

      deepest_ent_parent_id = deepest_ent_file_id;
      deepest_ent_file_id = UG_inode_file_id( inode );

      depth++;
   }

   itr_error = fskit_path_iterator_error( itr );

   // done iterating
   fskit_path_iterator_release( itr );

   // failed?
   if( rc != 0 ) {

      return rc;
   }

   // should have hit ENOENT if we had anything remote
   if( itr_error == 0 ) {

      // nothing to do!
      return 0;
   }
   else if( itr_error != -ENOENT ) {

      // some other error...
      SG_error("fskit_path_iterator_error('%s') rc = %d\n", fs_path, itr_error );
      return itr_error;
   }

   // build the head of the remote path
   // the first name is the first non-local entry
   remote_head = SG_strdup_or_null( fs_path );
   if( remote_head == NULL ) {

      return -ENOMEM;
   }

   rc = fskit_path_split( remote_head, &names );
   if( rc != 0 ) {

      SG_safe_free( remote_head );
      return -ENOMEM;
   }

   // head is the deepest local entry, who's child is remote
   rc = ms_client_path_download_ent_head( &deepest_ent, volume_id, deepest_ent_file_id, deepest_ent_parent_id, names[depth-1], NULL );

   if( rc != 0 ) {

      // OOM
      SG_safe_free( remote_head );
      SG_safe_free( names );
      return rc;
   }

   try {

      path_remote->push_back( deepest_ent );
   }
   catch( bad_alloc& ba ) {

      ms_client_free_path_ent( &deepest_ent, NULL );
      SG_safe_free( remote_head );
      SG_safe_free( names );
      return -ENOMEM;
   }

   // build the tail
   for( size_t i = depth; names[i] != NULL; i++ ) {

      struct ms_path_ent ms_ent;

      // skip .
      if( strcmp(names[i], ".") == 0 ) {
         continue;
      }

      rc = ms_client_path_download_ent_tail( &ms_ent, volume_id, names[i], NULL );
      if( rc != 0 ) {

         ms_client_free_path( path_remote, NULL );
         SG_safe_free( remote_head );
         SG_safe_free( names );
         return rc;
      }

      try {

         path_remote->push_back( ms_ent );
      }
      catch( bad_alloc& ba ) {

         ms_client_free_path( path_remote, NULL );
         SG_safe_free( remote_head );
         SG_safe_free( names );
         return -ENOMEM;
      }
   }

   SG_safe_free( remote_head );
   SG_safe_free( names );

   // built!
   return 0;
}


/**
 * @brief Clean up a remote path entry
 *
 * If it contains anything, it will be an fskit_xattr_set.  Free it.
 */
static void UG_consistency_path_free_remote( void* cls ) {

    if( cls != NULL ) {

        fskit_xattr_set* xattrs = (fskit_xattr_set*)cls;
        fskit_xattr_set_free( xattrs );
    }
}


/**
 * @brief Reload the path's-worth of metadata
 *
 * Cached path entries will be revalidated--reloaded, or dropped if they are no longer present upstream.
 * Un-cached path entries will be downloaded and grafted into the fskit filesystem
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 * @retval -errno Failure to connect
 */
int UG_consistency_path_ensure_fresh( struct SG_gateway* gateway, char const* fs_path ) {

   int rc = 0;
   bool not_found = false;      // set if we get ENOENT on a remote path

   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );

   struct ms_client* ms = SG_gateway_ms( gateway );

   ms_path_t path_local;
   ms_path_t path_remote;

   struct timespec refresh_start;

   struct ms_client_multi_result remote_inodes_stale;           // for revalidating stale data
   struct ms_client_multi_result remote_inodes_downloaded;      // for fetching unexplored data

   memset( &remote_inodes_stale, 0, sizeof(struct ms_client_multi_result) );
   memset( &remote_inodes_downloaded, 0, sizeof(struct ms_client_multi_result) );

   struct fskit_entry* graft_root = NULL;

   clock_gettime( CLOCK_REALTIME, &refresh_start );

   // find all local stale nodes.
   // each entry in path_local will be bound to its ref'ed fskit_entry
   rc = UG_consistency_path_find_local_stale( gateway, fs_path, &refresh_start, &path_local );
   if( rc != 0 ) {

      SG_error("UG_consistency_path_find_local_stale( '%s' ) rc = %d\n", fs_path, rc );
      return rc;
   }

   SG_debug("Will fetch %zu stale inodes for '%s'\n", path_local.size(), fs_path );

   // refresh stale data
   rc = ms_client_getattr_multi( ms, &path_local, &remote_inodes_stale );

   if( rc != 0 && remote_inodes_stale.reply_error != -ENOENT ) {

      UG_consistency_path_free( fs, &path_local );

      SG_error("ms_client_getattr_multi('%s') rc = %d, MS reply error %d\n", fs_path, rc, remote_inodes_stale.reply_error );
      return rc;
   }
   else if( remote_inodes_stale.reply_error == -ENOENT ) {

      not_found = true;
   }

   /////////////////////////////////////////////////////////////

   SG_debug("Fetched %d stale inodes for '%s'\n", remote_inodes_stale.num_processed, fs_path );
   for( int i = 0; i < remote_inodes_stale.num_processed; i++ ) {

      char* inode_str = NULL;
      if( remote_inodes_stale.ents[i].error == MS_LISTING_NEW ) {
         rc = md_entry_to_string( &remote_inodes_stale.ents[i], &inode_str );
         if( rc == 0 ) {

            SG_debug("NEW entry %d:\n%s\n", i, inode_str );
            SG_safe_free( inode_str );
         }
      }
   }

   /////////////////////////////////////////////////////////////

   // load downloaded inodes into the fskit filesystem tree
   if( remote_inodes_stale.num_processed > 0 ) {

      // prune absent entries and reload still-existing ones
      rc = UG_consistency_path_stale_reload( gateway, fs_path, &path_local, remote_inodes_stale.ents, remote_inodes_stale.num_processed );

      ms_client_multi_result_free( &remote_inodes_stale );
      UG_consistency_path_free( fs, &path_local );

      if( rc != 0 ) {

         SG_error("UG_consistency_path_stale_reload('%s') rc = %d\n", fs_path, rc );
         return rc;
      }
   }
   else {
       ms_client_multi_result_free( &remote_inodes_stale );
       UG_consistency_path_free( fs, &path_local );
   }

   if( not_found ) {

       // done
       ms_client_multi_result_free( &remote_inodes_stale );
       return -ENOENT;
   }

   // which inodes do we not have locally cached?
   rc = UG_consistency_path_find_remote( gateway, fs_path, &path_remote );
   if( rc != 0 ) {

      SG_error("UG_consistency_path_find_remote('%s') rc = %d\n", fs_path, rc );
      return rc;
   }

   SG_debug("Will fetch %zu remote inodes for '%s'\n", path_remote.size(), fs_path );

   // are any remote?
   if( path_remote.size() == 0 ) {

      // done!
      return 0;
   }

   // fetch remote inodes
   rc = ms_client_path_download( ms, &path_remote, &remote_inodes_downloaded );
   if( rc != 0 && rc != -ENOENT ) {

      ms_client_free_path( &path_remote, NULL );
      ms_client_multi_result_free( &remote_inodes_downloaded );

      SG_error("ms_client_download_path('%s') rc = %d\n", fs_path, rc );

      return rc;
   }
   else if( rc == -ENOENT ) {

      not_found = true;
   }

   // fetch the xattrs for all remote inodes we received for which we are the coordinator.
   // we will have received the xattr hash in the remote_inodes_downloaded.ents listing.
   // the xattrs in each case will be attached to path_remote's entries
   rc = UG_consistency_fetchxattrs_all( gateway, &path_remote, &remote_inodes_downloaded );
   if( rc != 0 ) {

      ms_client_free_path( &path_remote, UG_consistency_path_free_remote );
      ms_client_multi_result_free( &remote_inodes_downloaded );

      SG_error("UG_consistency_fetchxattrs_all('%s') rc = %d\n", fs_path, rc );

      return rc;
   }

   SG_debug("Fetched %d remote inode(s) for '%s'\n", remote_inodes_downloaded.num_processed, fs_path );

   // build a graft from all absent entries downloaded, as well as any xattrs we just downloaded
   rc = UG_consistency_fskit_path_graft_build( gateway, &path_remote, remote_inodes_downloaded.ents, remote_inodes_downloaded.num_processed, &graft_root );

   if( rc != 0 ) {

      ms_client_free_path( &path_remote, UG_consistency_path_free_remote );
      ms_client_multi_result_free( &remote_inodes_downloaded );

      SG_error("UG_consistency_fskit_path_graft_build('%s') rc = %d\n", fs_path, rc );
      return rc;
   }

   // graft absent inodes into fskit
   if( graft_root != NULL ) {

       rc = UG_consistency_fskit_path_graft_attach( gateway, fs_path, path_remote.at(0).parent_id, remote_inodes_downloaded.ents[0].name, graft_root );
       if( rc != 0 ) {

           ////////////////////////////////////////////////////////////////////
           SG_error("UG_consistency_fskit_path_graft_attach('%s' (at %" PRIX64 " (%s)) ) rc = %d\n", fs_path, fskit_entry_get_file_id( graft_root ), remote_inodes_downloaded.ents[0].name, rc );
           ////////////////////////////////////////////////////////////////////

           UG_consistency_fskit_path_graft_free( fs, graft_root, remote_inodes_downloaded.ents, remote_inodes_downloaded.num_processed );
           ms_client_multi_result_free( &remote_inodes_downloaded );

           ms_client_free_path( &path_remote, UG_consistency_path_free_remote );

           return rc;
       }
   }

   // finished!
   ms_client_free_path( &path_remote, NULL );
   ms_client_multi_result_free( &remote_inodes_downloaded );

   if( not_found ) {
       return -ENOENT;
   }
   else {
       return 0;
   }
}


/**
 * @brief Refresh a single inode's metadata
 * @attention inode->entry must NOT be locked if locked is False.  If locked is True, then parent must be given
 * @retval 0 The inode is already fresh, or is not changed remotely
 * @retval 1 The inode was not fresh, but we fetched and merged the new data successfully
 * @retval -ESTALE Unable to remotely refresh the inode, and it was remotely stale
 * @retval -errno Failure
 */
int UG_consistency_inode_ensure_fresh_ex( struct SG_gateway* gateway, char const* fs_path, struct UG_inode* inode, bool locked, struct fskit_entry* dent ) {

   int rc = 0;
   ms_path_t ms_inode;
   uint64_t volume_id = 0;
   uint64_t file_id = 0;
   int64_t file_version = 0;
   int64_t write_nonce = 0;
   uint64_t coordinator_id = 0;
   struct timespec now;
   struct ms_path_ent path_ent;
   struct md_entry entry;
   struct fskit_entry* fent = NULL;
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct ms_client* ms = SG_gateway_ms( gateway );
   struct fskit_core* fs = UG_state_fs( ug );
   bool need_request_refresh = false;
   uint64_t user_id = 0;
   char* fs_dirpath = md_dirname( fs_path, NULL );
   char* fent_name = md_basename( fs_path, NULL );

   if( fent_name == NULL || fs_dirpath == NULL ) {

      SG_safe_free( fent_name );
      SG_safe_free( fs_dirpath );
      return -ENOMEM;
   }

   memset( &entry, 0, sizeof(struct md_entry) );
   clock_gettime( CLOCK_REALTIME, &now );

   if( !locked ) {
       fskit_entry_rlock( UG_inode_fskit_entry( inode ));
   }

   volume_id = UG_inode_volume_id( inode );
   file_id = UG_inode_file_id( inode );
   coordinator_id = UG_inode_coordinator_id( inode );
   file_version = UG_inode_file_version( inode );
   write_nonce = UG_inode_write_nonce( inode );
   user_id = SG_gateway_user_id( gateway );

   need_request_refresh = (UG_inode_is_write_stale( inode, &now ) && coordinator_id != SG_gateway_id(gateway) && user_id != SG_USER_ANON);
   if( !need_request_refresh && !UG_inode_is_read_stale( inode, &now ) ) {

      // still fresh
      if( !locked ) {
          fskit_entry_unlock( UG_inode_fskit_entry( inode ) );
      }
      SG_safe_free( fent_name );
      SG_safe_free( fs_dirpath );

      SG_debug("Will NOT refresh %s (%" PRIX64 ")\n", fs_path, file_id);
      return 0;
   }

   SG_debug("Will refresh %s (%" PRIX64 ")\n", fs_path, file_id);

   if( !locked ) {
       fskit_entry_unlock( UG_inode_fskit_entry( inode ) );
   }

   if( need_request_refresh ) {
      SG_debug("Requesting remote fresh on %" PRIX64 "\n", file_id );
      rc = UG_consistency_request_refresh( gateway, fs_path );
      if( rc != 0 ) {

         SG_error("UG_consistency_request_refresh('%s') rc = %d\n", fs_path, rc);
         SG_safe_free( fent_name );
         SG_safe_free( fs_dirpath );
         return -ESTALE;
      }
      SG_debug("Remotely refreshed %" PRIX64 "\n", file_id);
   }


   rc = ms_client_getattr_request( &path_ent, volume_id, file_id, file_version, write_nonce, NULL);
   if( rc != 0 ) {

      // OOM
      SG_safe_free( fent_name );
      SG_safe_free( fs_dirpath );
      return rc;
   }

   rc = ms_client_getattr( ms, &path_ent, &entry );
   if( rc != 0 ) {

      SG_safe_free( fent_name );
      SG_safe_free( fs_dirpath );
      SG_error("ms_client_getattr(%" PRIX64 ") rc = %d, MS reply error %d\n", file_id, rc, entry.error );
      return rc;
   }

   if( entry.error == MS_LISTING_NOCHANGE ) {

      // we're fresh
      SG_safe_free( fent_name );
      SG_safe_free( fs_dirpath );
      md_entry_free( &entry );
      SG_debug("Entry %" PRIX64 " is fresh\n", file_id );

      // mark as such if not intermittently changed
      if( !locked ) {
          fskit_entry_wlock( UG_inode_fskit_entry(inode) );
      }

      if( UG_inode_file_version(inode) == file_version && UG_inode_write_nonce(inode) == write_nonce && UG_inode_coordinator_id(inode) == coordinator_id ) {
         // no change
         UG_inode_set_read_stale( inode, false );
         UG_inode_set_refresh_time_now( inode );
      }

      if( !locked ) {
          fskit_entry_unlock( UG_inode_fskit_entry(inode) );
      }

      return 0;
   }

   // write-lock both the parent and child, so we can reload
   // if the given entry is already locked, then we have to assume that the
   // parent is also write-locked
   if( !locked ) {
       if( dent != NULL ) {
          SG_error("dent = %p\n", dent);
          exit(1);
       }

       dent = fskit_entry_resolve_path( fs, fs_dirpath, 0, 0, true, &rc );

       if( dent == NULL ) {

          // this entry does not exist anymore...
          SG_safe_free( fent_name );
          SG_safe_free( fs_dirpath );
          md_entry_free( &entry );
          return rc;
       }
   }
   else if( dent == NULL ) {
      SG_error("dent = %p\n", dent);
      exit(1);
   }

   fent = fskit_dir_find_by_name( dent, fent_name );
   if( fent == NULL ) {

      // not found
      SG_safe_free( fent_name );
      SG_safe_free( fs_dirpath );
      md_entry_free( &entry );

      if( !locked ) {
          fskit_entry_unlock( dent );
      }

      return -ENOENT;
   }

   if( fent != UG_inode_fskit_entry(inode) ) {
      // different
      SG_error("BUG: inode mismatch: %p != %p\n", fent, UG_inode_fskit_entry(inode));
      SG_safe_free( fent_name );
      SG_safe_free( fs_dirpath );
      md_entry_free( &entry );

      if( !locked ) {
          fskit_entry_unlock( dent );
      }
      return -ESTALE;
   }

   if( !locked && fent != dent ) {
       fskit_entry_wlock( fent );
   }

   SG_debug("Reload: '%s' (%" PRIu64 ")\n", fs_path, entry.file_id );
   rc = UG_consistency_inode_reload( gateway, fs_path, dent, fent, fent_name, &entry );

   if( !locked && fent != dent ) {
       fskit_entry_unlock( fent );
   }

   if( !locked ) {
       fskit_entry_unlock( dent );
   }

   SG_safe_free( fent_name );
   SG_safe_free( fs_dirpath );
   md_entry_free( &entry );

   if( rc != 0 ) {

      SG_error("UG_consistency_inode_reload(%" PRIX64 ") rc = %d\n", file_id, rc );
      return rc;
   }

   return 1;
}

/**
 * @brief Ensure a locally-cached inode is fresh
 *
 * Call UG_consistency_inode_ensure_fresh_ex and return result
 * @see UG_consistency_inode_ensure_fresh_ex
 */
int UG_consistency_inode_ensure_fresh( struct SG_gateway* gateway, char const* fs_path, struct UG_inode* inode ) {
   return UG_consistency_inode_ensure_fresh_ex( gateway, fs_path, inode, false, NULL );
}

/**
 * @brief Merge a list of md_entrys into an fskit_entry directory.
 *
 * For conflicts, if a local entry is newer than the given cut-off, keep it.  Otherwise replace it.
 * @attention dent must be write-locked!
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 */
static int UG_consistency_dir_merge( struct SG_gateway* gateway, char const* fs_path_dir, struct fskit_entry* dent, struct md_entry* ents, size_t num_ents, struct timespec* keep_cutoff ) {

   int rc = 0;
   int type = 0;

   char* fs_path = NULL;
   size_t max_name_len = 0;

   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );
   struct fskit_dir_entry** existing = NULL;
   uint64_t num_existing = 0;

   int64_t ctime_sec = 0;
   int32_t ctime_nsec = 0;

   struct timespec ctime;

   // set up the fs_path buffer
   for( size_t i = 0; i < num_ents; i++ ) {

      if( ents[i].name != NULL ) {
         size_t len = strlen( ents[i].name );
         if( len > max_name_len ) {
             max_name_len = len;
         }
      }
   }

   fs_path = SG_CALLOC( char, strlen(fs_path_dir) + 1 + max_name_len + 2 );
   if( fs_path == NULL ) {

      return -ENOMEM;
   }

   set<string> ent_listing_names;

   for( size_t i = 0; i < num_ents; i++ ) {

      struct md_entry* ent = &ents[i];

      if( ent->name == NULL ) {
         continue;
      }

      ent_listing_names.insert( string(ent->name) );

      struct fskit_entry* fent = fskit_dir_find_by_name( dent, ent->name );

      if( fent != NULL ) {

         fskit_fullpath( fs_path_dir, ent->name, fs_path );

         fskit_entry_wlock( fent );

         // is this entry still active?
         type = fskit_entry_get_type(fent);
         if( type == FSKIT_ENTRY_TYPE_DEAD || fskit_entry_get_deletion_in_progress(fent) ) {
             SG_debug("Child '%s' is dead already\n", ent->name);

             rc = fskit_entry_try_garbage_collect(fs, fs_path, dent, fent);
             if( rc == 0 ) {
                // not destroyed yet
                fskit_entry_unlock(fent);
             }

             fent = NULL;
         }

         if( fent != NULL ) {
            // do we replace?
            // when was this entry created?
            fskit_entry_get_ctime( fent, &ctime_sec, &ctime_nsec );

            ctime.tv_sec = ctime_sec;
            ctime.tv_nsec = ctime_nsec;

            if( md_timespec_diff_ms( &ctime, keep_cutoff ) < 0 ) {

               SG_debug("Reload child '%s' with new listing\n", ent->name);

               // fent was created before the reload, and is in conflict.  reload
               rc = UG_consistency_inode_reload( gateway, fs_path, dent, fent, ent->name, ent );
               if( rc < 0 ) {

                  SG_error("UG_consistency_inode_reload('%s') rc = %d\n", fs_path, rc );

                  // try to soldier on...
                  rc = 0;

                  fskit_entry_unlock( fent );
               }
               else if( rc == 0 ) {

                  // reloaded, but not replaced
                  fskit_entry_unlock( fent );
               }
            }

            else {

               // preserve this entry
               SG_debug("Preserve child '%s' with existing listing\n", ent->name);
               fskit_entry_unlock( fent );
            }
         }
      }
      if( fent == NULL ) {

         // insert this entry
         fent = fskit_entry_new();
         if( fent == NULL ) {

            rc = -ENOMEM;
            break;
         }

         SG_debug("Insert '%s' into '%s'\n", ent->name, fs_path_dir );
         rc = UG_inode_fskit_entry_init( fs, fent, dent, ent );
         if( rc != 0 ) {

            SG_error("UG_inode_fskit_entry_init('%s') rc = %d\n", fs_path, rc );

            fskit_entry_destroy( fs, fent, false );
            SG_safe_free( fent );
            break;
         }

         rc = fskit_entry_attach_lowlevel( dent, fent, ent->name );
         if( rc != 0 ) {

            SG_error("fskit_entry_attach_lowlevel('%s', '%s') rc = %d\n", fs_path_dir, ent->name, rc );

            fskit_entry_destroy( fs, fent, false );
            SG_safe_free( fent );
            break;
         }
      }
   }

   // find the children that are no longer present, and older than the remove time, and remove them
   existing = fskit_listdir_locked(fs, dent, &num_existing, &rc);
   if( existing == NULL ) {
      SG_error("fskit_listdir_locked('%s') rc = %d\n", fs_path_dir, rc);
      return rc;
   }

   for( uint64_t i = 0; i < num_existing; i++ ) {

      struct fskit_dir_entry* dirent = existing[i];
      if( strcmp(dirent->name, ".") == 0 || strcmp(dirent->name, "..") == 0 ) {
         continue;
      }

      // absent now?
      if( ent_listing_names.find(string(dirent->name)) == ent_listing_names.end() ) {

         struct fskit_entry* child = fskit_dir_find_by_name(dent, dirent->name);
         if( child == NULL ) {
            SG_debug("Already absent: '%s'\n", dirent->name);
            continue;
         }

         SG_debug("Entry '%s' is no longer present.  Removing...\n", dirent->name);

         char* fp = fskit_fullpath(fs_path_dir, dirent->name, NULL);
         if( fp == NULL ) {
            fskit_dir_entry_free_list(existing);
            SG_safe_free(fs_path);
            return -ENOMEM;
         }

         fskit_entry_wlock(child);

         rc = UG_deferred_remove(ug, fp, child);

         fskit_entry_unlock(child);

         SG_safe_free(fp);

         if( rc != 0 ) {
            SG_error("UG_deferred_remove(%s) rc = %d\n", fp, rc);
         }
      }
   }

   fskit_dir_entry_free_list(existing);
   SG_safe_free( fs_path );
   return rc;
}


/**
 * @brief Ensure that a directory has a fresh listing of children
 *
 * If not, fetch the immediate children, the named directory, and attach them all
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 * @todo Adjustments necessary for forcing listdir in UG_consistency_dir_ensure_fresh
 */
int UG_consistency_dir_ensure_fresh( struct SG_gateway* gateway, char const* fs_path ) {

   int rc = 0;
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );
   struct UG_inode* inode = NULL;

   uint64_t file_id = 0;
   int64_t num_children = 0;
   int64_t least_unknown_generation = 0;
   int32_t max_read_freshness = 0;
   int64_t capacity = 0;

   struct timespec now;
   struct timespec dir_refresh_time;
   struct timespec children_refresh_time;

   struct ms_client_multi_result results;
   memset( &results, 0, sizeof(struct ms_client_multi_result) );

   char const* method = NULL;

   struct ms_client* ms = SG_gateway_ms( gateway );

   struct fskit_entry* dent = fskit_entry_resolve_path( fs, fs_path, 0, 0, true, &rc );
   if( dent == NULL ) {

      return rc;
   }

   rc = clock_gettime( CLOCK_REALTIME, &now );
   if( rc != 0 ) {

      rc = -errno;
      fskit_entry_unlock( dent );

      SG_error("clock_gettime rc = %d\n", rc );
      return rc;
   }


   inode = (struct UG_inode*)fskit_entry_get_user_data( dent );

   dir_refresh_time = UG_inode_refresh_time( inode );
   max_read_freshness = UG_inode_max_read_freshness( inode );
   children_refresh_time = UG_inode_children_refresh_time( inode );

   // is the inode's directory listing still fresh?
   if( md_timespec_diff_ms( &now, &dir_refresh_time ) <= max_read_freshness && md_timespec_diff_ms( &now, &children_refresh_time ) <= max_read_freshness ) {

      // still fresh
      SG_debug("Directory '%s' is fresh\n", fs_path );
      fskit_entry_unlock( dent );
      return 0;
   }

   SG_debug("Refresh directory '%s'\n", fs_path);

   // stale--redownload
   file_id = fskit_entry_get_file_id( dent );
   num_children = UG_inode_ms_num_children( inode );
   least_unknown_generation = UG_inode_generation( inode );
   capacity = UG_inode_ms_capacity( inode );

   // reference dent--it must stick around
   fskit_entry_ref_entry( dent );

   fskit_entry_unlock( dent );

   // have we listed before?
   // TODO: force listdir for now
   if( least_unknown_generation <= 1 || 1 ) {

      // nope--full download
      method = "ms_client_listdir";
      SG_debug("ms_client_listdir(%" PRIX64 ", %" PRId64 ", %" PRId64 ")\n", file_id, num_children, capacity);
      rc = ms_client_listdir( ms, file_id, num_children, capacity, &results );
   }
   else {

      method = "ms_client_diffdir";
      SG_debug("ms_client_diffdir(%" PRIX64 ", %" PRId64 ", %" PRId64 ")\n", file_id, num_children, least_unknown_generation + 1);
      rc = ms_client_diffdir( ms, file_id, num_children, least_unknown_generation + 1, &results );
   }

   if( rc < 0 ) {

      SG_error("%s('%s') rc = %d\n", method, fs_path, rc );
      fskit_entry_unref( fs, fs_path, dent );

      ms_client_multi_result_free( &results );

      return rc;
   }

   if( results.reply_error != 0 ) {

      SG_error("%s('%s') reply_error = %d\n", method, fs_path, rc );
      fskit_entry_unref( fs, fs_path, dent );

      ms_client_multi_result_free( &results );

      return rc;
   }

   SG_debug("%s('%s') succeeded; merging %zu entries\n", method, fs_path, results.num_ents);

   // re-acquire
   fskit_entry_wlock( dent );

   // load them in
   rc = UG_consistency_dir_merge( gateway, fs_path, dent, results.ents, results.num_ents, &now );

   if( rc == 0 ) {

      // set refresh time
      UG_inode_set_children_refresh_time_now( inode );
   }

   fskit_entry_unlock( dent );

   ms_client_multi_result_free( &results );

   if( rc != 0 ) {

      SG_error("UG_consistency_dir_merge('%s') rc = %d\n", fs_path, rc );
   }

   fskit_entry_unref( fs, fs_path, dent );
   return rc;
}


/**
 * @brief Fetch all xattrs for a file inode.
 *
 * This is necessary for when we are the coordinator of the file, or are about to become it.
 * @param[out] *ret_xattr Set to include extended attribute names, values, and value lengths
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 * @retval -ENODATA Failed to fetch the xattr bundle from the MS
 * @retval -errno Network-level error
 */
int UG_consistency_fetchxattrs( struct SG_gateway* gateway, uint64_t file_id, int64_t xattr_nonce, unsigned char* xattr_hash, fskit_xattr_set** ret_xattrs ) {

   int rc = 0;
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t volume_id = ms_client_get_volume_id( ms );

   char** xattr_names = NULL;
   char** xattr_values = NULL;
   size_t* xattr_value_lengths = NULL;
   fskit_xattr_set* xattr_set = NULL;

   rc = ms_client_fetchxattrs( ms, volume_id, file_id, xattr_nonce, xattr_hash, &xattr_names, &xattr_values, &xattr_value_lengths );
   if( rc != 0 ) {

      SG_error("ms_client_fetchxattrs(/%" PRIu64 "/%" PRIX64 ".%" PRId64 ") rc = %d\n", volume_id, file_id, xattr_nonce, rc );
      return -ENODATA;
   }

   if( xattr_names[0] == NULL ) {
      // no xattrs
      *ret_xattrs = NULL;
      SG_FREE_LIST( xattr_names, free );
      SG_FREE_LIST( xattr_values, free );
      SG_safe_free( xattr_value_lengths );
      return 0;
   }

   for( size_t i = 0; xattr_names[i] != NULL; i++ ) {

      //////////////////////////////////////////////////////////////////
      char value_buf[25];
      memset( value_buf, 0, 25 );
      memcpy( value_buf, xattr_values[i], MIN(20, xattr_value_lengths[i]) );
      memcpy( value_buf +( MIN(20, xattr_value_lengths[i])), "...\0", 4);

      SG_debug("fetchxattr: '%s' = '%s' (length %zu)\n", xattr_names[i], value_buf, xattr_value_lengths[i] );
      //////////////////////////////////////////////////////////////////

      rc = fskit_xattr_set_insert( &xattr_set, xattr_names[i], xattr_values[i], xattr_value_lengths[i], 0 );
      if( rc != 0 ) {
         SG_error("fskit_xattr_set_insert rc = %d\n", rc );
         break;
      }
   }

   SG_FREE_LIST( xattr_names, free );
   SG_FREE_LIST( xattr_values, free );
   SG_safe_free( xattr_value_lengths );

   if( rc != 0 ) {

      fskit_xattr_set_free( xattr_set );
      return rc;
   }

   *ret_xattrs = xattr_set;
   return 0;
}


/**
 * @brief Fetch all xattrs for the files for which we are the coordinator, and merge them into the path.
 *
 * Remote_inodes->ents[i] will match path_remote->at(i), and we will put the resulting xattr bundle into path_remote->at(i)
 * We do not have the xattr hash for these nodes yet, so just go with the one from the signed MS entry we put there.
 * @retval 0 Success, and pair the fskit_xattr_set with each inode's data in the result.
 * @retval -ENOMEM Out of Memory
 * @retval -ENODATA Failed to fetch the xattr bundle from the MS, for whatever reason
 * @retval -errno Network-level error
 */
static int UG_consistency_fetchxattrs_all( struct SG_gateway* gateway, ms_path_t* path_remote, struct ms_client_multi_result* remote_inodes ) {

   int rc = 0;
   for( size_t i = 0; i < path_remote->size() && remote_inodes->num_processed > 0 && i < (unsigned)remote_inodes->num_processed; i++ ) {

      fskit_xattr_set* xattrs = NULL;

      // only do this if we're the coordinator, and if there is xattr data at all
      if( SG_gateway_id( gateway ) == remote_inodes->ents[i].coordinator && remote_inodes->ents[i].xattr_hash != NULL ) {

         SG_debug("Fetch xattrs for %" PRIX64 ".%" PRId64 ".%" PRId64 "\n", remote_inodes->ents[i].file_id, remote_inodes->ents[i].version, remote_inodes->ents[i].xattr_nonce );
         rc = UG_consistency_fetchxattrs( gateway, (*path_remote)[i].file_id, remote_inodes->ents[i].xattr_nonce, remote_inodes->ents[i].xattr_hash, &xattrs );
         if( rc != 0 ) {

             SG_error("UG_consistency_fetchxattrs(%" PRIX64 ") rc = %d\n", (*path_remote)[i].file_id, rc );
             return rc;
         }

         // associate the xattrs with this path entry
         ms_client_path_ent_set_cls( &path_remote->at(i), xattrs );
      }
   }

   return 0;
}


/**
 * @brief Ask a remote coordinator to verify that its data is still present
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 * @retval -EINVAL This is the coordinator
 * @retval -EREMOTEIO Network error
 * @retval -EAGAIN Timeout
 * @retval !0 Error code from the remote refresh if it failed
 */
int UG_consistency_request_refresh( struct SG_gateway* gateway, char const* fs_path ) {

   int rc = 0;
   SG_messages::Request req;
   SG_messages::Reply reply;
   struct SG_request_data reqdat;
   struct fskit_entry* fent = NULL;
   struct UG_inode* inode = NULL;
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );

   int64_t manifest_mtime_sec = 0;
   int32_t manifest_mtime_nsec = 0;
   uint64_t file_id = 0;
   int64_t file_version = 0;

   fent = fskit_entry_resolve_path( fs, fs_path, 0, 0, false, &rc );
   if( fent == NULL ) {
      SG_error("fskit_entry_resolve_path('%s') rc = %d\n", fs_path, rc);
      return rc;
   }

   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   file_id = UG_inode_file_id( inode );
   file_version = UG_inode_file_version( inode );

   // can't do this if we're the coordinator
   if( SG_gateway_id( gateway ) == UG_inode_coordinator_id( inode ) ) {
      SG_error("We're the coordinator of '%s'\n", fs_path );
      fskit_entry_unlock( fent );
      return -EINVAL;
   }

   SG_manifest_get_modtime( UG_inode_manifest( inode ), &manifest_mtime_sec, &manifest_mtime_nsec );

   rc = SG_request_data_init_manifest( gateway, fs_path, UG_inode_file_id( inode ), UG_inode_file_version( inode ), manifest_mtime_sec, manifest_mtime_nsec, &reqdat );
   if( rc != 0 ) {

      // OOM
      fskit_entry_unlock( fent );
      return rc;
   }

   rc = SG_client_request_REFRESH_setup( gateway, &req, &reqdat, UG_inode_coordinator_id( inode ) );
   if( rc != 0 ) {

      // OOM
      fskit_entry_unlock( fent );
      SG_error("SG_client_request_REFRESH_setup('%s') rc = %d\n", fs_path, rc );
      SG_request_data_free( &reqdat );
      return rc;
   }

   SG_request_data_free( &reqdat );

   rc = SG_client_request_send( gateway, UG_inode_coordinator_id( inode ), &req, NULL, &reply );
   fskit_entry_unlock( fent );

   if( rc != 0 ) {

      // network error
      SG_error("SG_client_request_send(REFRESH '%s') rc = %d\n", fs_path, rc );

      // timed out? retry
      if( rc == -ETIMEDOUT ) {
         rc = -EAGAIN;
      }

      // propagate retries; everything else is remote I/O error
      if( rc != -EAGAIN ) {
         rc = -EREMOTEIO;
      }

      return rc;
   }

   if( reply.error_code() != 0 ) {

      // failed to process
      SG_error("SG_client_request_send(REFRESH '%s') reply error = %d\n", fs_path, rc );
      return reply.error_code();
   }

   // success! update refresh-time
   fent = fskit_entry_resolve_path( fs, fs_path, 0, 0, true, &rc );
   if( fent == NULL ) {
      SG_error("fskit_entry_resolve_path('%s') rc = %d\n", fs_path, rc);
      if( rc == -ENOENT ) {
         // file disappeared during our reload (but that's okay)
         return 0;
      }
      return rc;
   }

   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );

   // did the file change while we made our request?
   if( UG_inode_file_id( inode ) != file_id ) {
      fskit_entry_unlock( fent );
      goto UG_consistency_request_refresh_out;
   }

   if( UG_inode_file_version( inode ) != file_version ) {
      fskit_entry_unlock( fent );
      goto UG_consistency_request_refresh_out;
   }

   UG_inode_set_write_refresh_time_now( inode );
   fskit_entry_unlock( fent );

UG_consistency_request_refresh_out:
   return rc;
}

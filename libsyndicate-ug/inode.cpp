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
 * @file libsyndicate-ug/inode.cpp
 * @author Jude Nelson
 * @date 9 Mar 2016
 *
 * @brief User Gateway inode related functions
 *
 * @see libsyndicate-ug/inode.h
 */

#include "inode.h"
#include "block.h"

/// UG-specific inode information, for fskit
struct UG_inode {

   char* name;                                  ///< Name of this inode (allowed, since Syndicate does not support links)
   struct SG_manifest manifest;                 ///< Latest manifest of this file's blocks (includes coordinator_id and file_version)

   unsigned char ms_xattr_hash[ SHA256_DIGEST_LENGTH ]; ///< Latest xattr hash
   int64_t generation;                          ///< Last-known generation number of this file

   int64_t write_nonce;                         ///< Latest write nonce
   int64_t xattr_nonce;                         ///< Latest xattr nonce

   struct timespec refresh_time;                ///< Time of last refresh from the ms
   struct timespec write_refresh_time;          ///< Time of last remote-fresh request to the file's coordinator
   struct timespec manifest_refresh_time;       ///< Time of last manifest refresh
   struct timespec children_refresh_time;       ///< If this is a directory, this is the time the children were last reloaded
   int32_t max_read_freshness;                  ///< How long since last refresh, in millis, this inode is to be considered fresh for reading
   int32_t max_write_freshness;                 ///< How long since last refresh, in millis, this inode is to be considered fresh from a remote update (0 means "always fresh")

   bool read_stale;                             ///< If true, this file must be revalidated before the next read
   bool write_stale;                            ///< If true, this file must be revalidated before the next write
   bool dirty;                                  ///< If true, then we need to flush data on fsync()

   int64_t ms_num_children;                     ///< The number of children the MS says this inode has
   int64_t ms_capacity;                         ///< Maximum index number of a child in the MS

   bool vacuuming;                              ///< If true, then we're currently vacuuming this file
   bool vacuumed;                               ///< If true, then we've already tried to vacuum this file upon discovery (false means we should try again)

   UG_dirty_block_map_t* dirty_blocks;          ///< Set of locally-modified blocks that must be replicated, either on the next fsync() or last close()

   struct SG_manifest replaced_blocks;          ///< Set of blocks replaced by writes, that need to be garbage-collected (contains only metadata)

   UG_inode_fsync_queue_t* sync_queue;          ///< Queue of fsync requests on this inode

   struct fskit_entry* entry;                   ///< The fskit entry that owns this inode

   bool renaming;                               ///< If true, then this inode is in the process of getting renamed.  Concurrent renames will fail with EBUSY
   bool deleting;                               ///< If true, then this inode is in the process of being deleted.  Concurrent opens and stats will fail
   bool creating;                               ///< If true, then this inode is in the process of being created.  Truncate will be a no-op in this case.
};


/**
 * @brief Read lock an inode, using its fskit entry
 *
 * This is meant for external API consumers, like other gateways.
 * @return Result of fskit_entry_rlock
 * @retval -EINVAL inode->entry is NULL
 * @see fskit_entry_rlock
 */
int UG_inode_rlock( struct UG_inode* inode ) {
    if( inode->entry == NULL ) {
       return -EINVAL;
    }
    return fskit_entry_rlock( inode->entry );
}

/**
 * @brief Write lock an inode, using its fskit entry
 *
 * This is meant for external API consumers, like other gateways.
 * @return Result of fskit_entry_wlock
 * @retval -EINVAL inode->entry is NULL
 * @see fskit_entry_wlock
 */
int UG_inode_wlock( struct UG_inode* inode ) {
   if( inode->entry == NULL ) {
      return -EINVAL;
   }
   return fskit_entry_wlock( inode->entry );
}


/**
 * @brief Unlock an inode, using its fskit entry
 *
 * This is meant for external API consumers, like other gateways.
 * @return Result of fskit_entry_unlock
 * @retval -EINVAL inode->entry is NULL
 * @see fskit_entry_unlock
 */
int UG_inode_unlock( struct UG_inode* inode ) {
   if( inode->entry == NULL ) {
      return -EINVAL;
   }
   return fskit_entry_unlock( inode->entry );
}


/**
 * @brief Initialize common inode data
 *
 * @param[in] type MD_ENTRY_FILE or MD_ENTRY_DIR
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 */
static int UG_inode_init_common( struct UG_inode* inode, char const* name, int type ) {

   memset( inode, 0, sizeof(struct UG_inode) );

   inode->name = SG_strdup_or_null( name );
   if( inode->name == NULL ) {

       if( name != NULL ) {
          return -ENOMEM;
       }
       else {
          return -EINVAL;
       }
   }

   // regular file?
   if( type == MD_ENTRY_FILE ) {

      // sync queue
      inode->sync_queue = SG_safe_new( UG_inode_fsync_queue_t() );
      if( inode->sync_queue == NULL ) {

         return -ENOMEM;
      }

      // dirty blocks
      inode->dirty_blocks = SG_safe_new( UG_dirty_block_map_t() );
      if( inode->dirty_blocks == NULL ) {

         SG_safe_delete( inode->sync_queue );
         return -ENOMEM;
      }
   }
   else {
      UG_inode_set_size( inode, 4096 );
   }

   return 0;
}

/**
 * @brief Initialize an inode, from an entry and basic data
 * @attention entry must be write-locked
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 */
int UG_inode_init( struct UG_inode* inode, char const* name, struct fskit_entry* entry, uint64_t volume_id, uint64_t coordinator_id, int64_t file_version ) {

   int rc = 0;

   rc = UG_inode_init_common( inode, name, fskit_entry_get_type( entry ) == FSKIT_ENTRY_TYPE_FILE ? MD_ENTRY_FILE : MD_ENTRY_DIR );
   if( rc != 0 ) {

      return rc;
   }

   // manifest
   rc = SG_manifest_init( &inode->manifest, volume_id, coordinator_id, fskit_entry_get_file_id( entry ), file_version );
   if( rc != 0 ) {

      SG_safe_delete( inode->sync_queue );
      SG_safe_delete( inode->dirty_blocks );
      return rc;
   }

   if( fskit_entry_get_type(entry) == FSKIT_ENTRY_TYPE_FILE ) {
       SG_manifest_set_size( &inode->manifest, fskit_entry_get_size( entry ) );
   }
   else {
       SG_manifest_set_size( &inode->manifest, 4096 );
   }

   if( fskit_entry_get_type( entry ) == FSKIT_ENTRY_TYPE_FILE ) {

      // replaced blocks
      rc = SG_manifest_init( &inode->replaced_blocks, volume_id, coordinator_id, fskit_entry_get_file_id( entry ), file_version );
      if( rc != 0 ) {

         SG_safe_delete( inode->sync_queue );
         SG_safe_delete( inode->dirty_blocks );
         SG_manifest_free( &inode->manifest );
         return rc;
      }
   }

   return 0;
}


/**
 * @brief Initialize an inode from an exported inode data and an fskit_entry
 * @attention File ID in inode_data and fent must match, as must their types
 * The inode's manifest will be stale, since it currently has no data.
 * fent must be at least read-locked
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 * @retval -EINVAL The data is invalid (i.e. file IDs don't match)
 */
int UG_inode_init_from_export( struct UG_inode* inode, struct md_entry* inode_data, struct fskit_entry* fent ) {

   int rc = 0;
   int type = fskit_entry_get_type( fent );
   uint64_t file_id = fskit_entry_get_file_id( fent );

   // ID sanity check
   if( inode_data->file_id != file_id && inode_data->file_id > 0 ) {
      SG_error("inode_data->file_id == %" PRIX64 ", fent->file_id == %" PRIX64 "\n", inode_data->file_id, file_id );
      return -EINVAL;
   }

   // type sanity check
   if( type == FSKIT_ENTRY_TYPE_FILE && inode_data->type > 0 && inode_data->type != MD_ENTRY_FILE ) {
      SG_error("inode_data->type == %d, fent->type == %d\n", inode_data->type, type );
      return -EINVAL;
   }

   if( type == FSKIT_ENTRY_TYPE_DIR && inode_data->type > 0 && inode_data->type != MD_ENTRY_DIR ) {
      SG_error("inode_data->type == %d, fent->type == %d\n", inode_data->type, type );
      return -EINVAL;
   }

   /////////////////////////////////////
   /*
   char* tmp = NULL;
   rc = md_entry_to_string( inode_data, &tmp );
   if( rc == 0 && tmp != NULL ) {
      SG_debug("Init '%s' from export with:\n%s\n", inode_data->name, tmp );
      SG_safe_free( tmp );
   }
   */
   /////////////////////////////////////

   rc = UG_inode_init( inode, inode_data->name, fent, inode_data->volume, inode_data->coordinator, inode_data->version );
   if( rc != 0 ) {

      return rc;
   }

   SG_manifest_set_modtime( &inode->manifest, inode_data->manifest_mtime_sec, inode_data->manifest_mtime_nsec );

   inode->write_nonce = inode_data->write_nonce;
   inode->xattr_nonce = inode_data->xattr_nonce;
   inode->generation = inode_data->generation;
   inode->max_read_freshness = inode_data->max_read_freshness;
   inode->max_write_freshness = inode_data->max_write_freshness;
   inode->ms_num_children = inode_data->num_children;
   inode->ms_capacity = inode_data->capacity;

   clock_gettime( CLOCK_REALTIME, &inode->refresh_time );

   SG_manifest_set_stale( &inode->manifest, true );

   return 0;
}


/**
 * @brief Common fskit entry initialization from an exported inode
 * @retval 0 Success (always succeeds)
 */
int UG_inode_fskit_common_init( struct fskit_entry* fent, struct md_entry* inode_data ) {

   struct timespec ts;

   // set ctime, mtime
   ts.tv_sec = inode_data->mtime_sec;
   ts.tv_nsec = inode_data->mtime_nsec;
   fskit_entry_set_mtime( fent, &ts );

   ts.tv_sec = inode_data->ctime_sec;
   ts.tv_nsec = inode_data->ctime_nsec;
   fskit_entry_set_ctime( fent, &ts );

   // set size
   if( inode_data->type == MD_ENTRY_DIR ) {
      fskit_entry_set_size( fent, 4096 );
   }
   else {
      fskit_entry_set_size( fent, inode_data->size );
   }

   return 0;
}

/**
 * @brief Generate a new fskit entry for a directory
 * @attention fent must be write-locked
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 * @retval -EINVAL inode_data doesn't represent a file
 */
static int UG_inode_fskit_dir_init( struct fskit_entry* fent, struct fskit_entry* parent, struct md_entry* inode_data ) {

   int rc = 0;

   // sanity check
   if( inode_data->type != MD_ENTRY_DIR ) {
      SG_error("Inode %" PRIX64 " is not a directory\n", inode_data->file_id);
      return -EINVAL;
   }

   rc = fskit_entry_init_dir( fent, parent, inode_data->file_id, inode_data->owner, inode_data->volume, inode_data->mode );
   if( rc != 0 ) {

      return rc;
   }

   UG_inode_fskit_common_init( fent, inode_data );

   return 0;
}

/**
 * @brief Generate a new fskit entry for a regular file
 * @attention fent must be write-locked
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 * @retval -EINVAL inode_data doesn't represent a file
*/
static int UG_inode_fskit_file_init( struct fskit_entry* fent, struct md_entry* inode_data ) {

   int rc = 0;

   // sanity
   if( inode_data->type != MD_ENTRY_FILE ) {
      return -EINVAL;
   }

   rc = fskit_entry_init_file( fent, inode_data->file_id, inode_data->owner, inode_data->volume, inode_data->mode );
   if( rc != 0 ) {

      return rc;
   }

   UG_inode_fskit_common_init( fent, inode_data );
   return 0;
}


/**
 * @brief Build an fskit entry from an exported inode
 * @attention fent must be write-locked
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 */
int UG_inode_fskit_entry_init( struct fskit_core* fs, struct fskit_entry* fent, struct fskit_entry* parent, struct md_entry* inode_data ) {

   int rc = 0;
   struct UG_inode* inode = NULL;

   // going from file to directory?
   if( inode_data->type == MD_ENTRY_FILE ) {

      // turn the inode into a directory, and blow away the file's data
      // set up the file
      rc = UG_inode_fskit_file_init( fent, inode_data );
      if( rc != 0 ) {

         SG_error("UG_inode_fskit_file_init('%s' (%" PRIX64 ")) rc = %d\n", inode_data->name, inode_data->file_id, rc );
         return rc;
      }
   }
   else {

      // going to make a directory, and replace this file.
      // set up the directory
      rc = UG_inode_fskit_dir_init( fent, parent, inode_data );
      if( rc != 0 ) {

         SG_error("UG_inode_fskit_dir_init('%s' (%" PRIX64 ")) rc = %d\n", inode_data->name, inode_data->file_id, rc );
         return rc;
      }
   }

   // build the inode
   inode = UG_inode_alloc( 1 );
   if( inode == NULL ) {

      return -ENOMEM;
   }

   // set up the inode
   rc = UG_inode_init_from_export( inode, inode_data, fent );
   if( rc != 0 ) {

      SG_error("UG_inode_init_from_export('%s' (%" PRIX64 ")) rc = %d\n", inode_data->name, inode_data->file_id, rc );

      SG_manifest_free( &inode->manifest );
      SG_safe_free( inode );
      return rc;
   }

   // put the inode into the fent, and fent into inode
   UG_inode_bind_fskit_entry( inode, fent );

   return 0;
}

/**
 * @brief Free an inode
 * @note Destroys its dirty blocks
 * @return 0
 */
int UG_inode_free( struct UG_inode* inode ) {

   SG_safe_free( inode->name );
   SG_safe_delete( inode->sync_queue );

   if( inode->dirty_blocks != NULL ) {
       UG_dirty_block_map_free( inode->dirty_blocks );
       SG_safe_delete( inode->dirty_blocks );
   }

   SG_manifest_free( &inode->manifest );
   SG_manifest_free( &inode->replaced_blocks );
   memset( inode, 0, sizeof(struct UG_inode) );

   return 0;
}


/**
 * @brief Set up a file handle
 * @attention inode->entry must be read-locked
 * @retval 0 Success
 * @retval -EINVAL The inode is malformed (NOTE: indicates a bug!)
 * @retval -ENOMEM Out of Memory
 */
int UG_file_handle_init( struct UG_file_handle* fh, struct UG_inode* inode, int flags ) {

   if( inode->entry == NULL ) {
      return -EINVAL;
   }

   fh->inode_ref = inode;
   fh->flags = flags;

   fh->block_size = 0;

   fh->read_buffer = NULL;

   fh->prefetch_queue = UG_read_prefetch_queue_new();
   UG_read_prefetch_queue_init(fh->prefetch_queue);

   fh->download_connection_pool = md_download_connection_pool_new();
   md_download_connection_pool_init(fh->download_connection_pool);
   md_download_connection_pool_set_user_data(fh->download_connection_pool, fh);
   md_download_connection_pool_set_event_func(fh->download_connection_pool, &UG_read_prefetch_download_connection_pool_event_handler);
   return 0;
}


/**
 * @brief Free a file handle
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 */
int UG_file_handle_free( struct UG_file_handle* fh ) {
    if(fh->read_buffer) {
        UG_read_buffer_free(fh->read_buffer);
        SG_safe_free(fh->read_buffer);
    }

    UG_read_prefetch_queue_free(fh->prefetch_queue);
    SG_safe_free(fh->prefetch_queue);

    md_download_connection_pool_free(fh->download_connection_pool);
    SG_safe_free(fh->download_connection_pool);

    memset( fh, 0, sizeof(struct UG_file_handle) );

    return 0;
}

/**
 * @brief Export all non-builtin xattrs for an inode.
 * @attention inode->entry must be read-locked
 * @param[out] *ret_xattr_names Extended attribute names
 * @param[out] *ret_xattr_values Extended attribute values
 * @param[out] *ret_xattr_value_lengths Extended attribute value lengths
 * @retval 0 Success, filling in *ret_xattr_names, *ret_xattr_values, and *ret_xattr_value_lengths if there are xattrs in this inode.
 * @retval -ENOMEM Out of Memory
 */
int UG_inode_export_xattrs( struct fskit_core* fs, struct UG_inode* inode, char*** ret_xattr_names, char*** ret_xattr_values, size_t** ret_xattr_value_lengths ) {

   int rc = 0;
   size_t num_xattrs = 0;
   ssize_t xattr_list_len = 0;
   char** xattr_names = NULL;
   char** xattr_values = NULL;
   size_t* xattr_value_lengths = NULL;
   char* xattr_list = NULL;
   char* xattr_ptr = NULL;

   xattr_list_len = fskit_xattr_flistxattr( fs, inode->entry, NULL, 0 );
   if( xattr_list_len < 0 ) {

      SG_error("fskit_flistxattr(NULL) rc = %zd\n", xattr_list_len );
      return (int)xattr_list_len;
   }

   SG_debug("xattr_list_len = %zd\n", xattr_list_len );
   if( xattr_list_len == 0 ) {

      // no xattrs
      *ret_xattr_names = NULL;
      *ret_xattr_values = NULL;
      *ret_xattr_value_lengths = NULL;
      return 0;
   }

   // get the list of xattrs
   xattr_list = SG_CALLOC( char, xattr_list_len );
   if( xattr_list == NULL ) {

      return -ENOMEM;
   }

   rc = fskit_xattr_flistxattr( fs, inode->entry, xattr_list, xattr_list_len );
   if( rc < 0 ) {

      SG_error( "fskit_flistxattr(%zd) rc = %d\n", xattr_list_len, rc );
      return rc;
   }

   // how many xattrs?
   for( ssize_t i = 0; i < xattr_list_len; i++ ) {

      if( xattr_list[i] == '\0' ) {
         num_xattrs++;
      }
   }

   SG_debug("num_xattrs = %zu\n", num_xattrs);
   if( num_xattrs == 0 ) {
      // trivial case: no xattrs
      goto UG_inode_export_xattrs_out;
   }

   xattr_names = SG_CALLOC( char*, num_xattrs + 1 );
   xattr_values = SG_CALLOC( char*, num_xattrs + 1 );
   xattr_value_lengths = SG_CALLOC( size_t, num_xattrs + 1 );

   if( xattr_names == NULL || xattr_values == NULL || xattr_value_lengths == NULL ) {
      SG_safe_free( xattr_names );
      SG_safe_free( xattr_values );
      SG_safe_free( xattr_value_lengths );
      SG_safe_free( xattr_list );
      return -ENOMEM;
   }

   // get each xattr
   xattr_ptr = xattr_list;
   for( size_t i = 0; i < num_xattrs; i++ ) {

      char* xattr_name = NULL;
      char* xattr_value = NULL;
      ssize_t xattr_value_len = 0;

      xattr_name = SG_strdup_or_null( xattr_ptr );
      if( xattr_name == NULL ) {

         rc = -ENOMEM;
         goto UG_inode_export_xattrs_fail;
      }

      xattr_value_len = fskit_xattr_fgetxattr( fs, inode->entry, xattr_name, NULL, 0 );
      if( xattr_value_len < 0 ) {

         rc = xattr_value_len;
         SG_safe_free( xattr_name );
         goto UG_inode_export_xattrs_fail;
      }

      xattr_value = SG_CALLOC( char, xattr_value_len );
      if( xattr_value == NULL ) {

         rc = -ENOMEM;
         SG_safe_free( xattr_name );
         goto UG_inode_export_xattrs_fail;
      }

      rc = fskit_xattr_fgetxattr( fs, inode->entry, xattr_name, xattr_value, xattr_value_len );
      if( rc < 0 ) {

         SG_safe_free( xattr_name );
         SG_safe_free( xattr_value );
         goto UG_inode_export_xattrs_fail;
      }

      xattr_names[i] = xattr_name;
      xattr_values[i] = xattr_value;
      xattr_value_lengths[i] = xattr_value_len;

      xattr_ptr += strlen(xattr_name) + 1;
   }

UG_inode_export_xattrs_out:

   *ret_xattr_names = xattr_names;
   *ret_xattr_values = xattr_values;
   *ret_xattr_value_lengths = xattr_value_lengths;
   SG_safe_free( xattr_list );

   return 0;

UG_inode_export_xattrs_fail:

   SG_FREE_LIST( xattr_names, free );
   SG_FREE_LIST( xattr_values, free );
   SG_safe_free( xattr_value_lengths );
   SG_safe_free( xattr_list );

   return rc;
}


/**
 * @brief Calculate the xattr hash for an inode
 * @param[out] *xattr_hash Hash which must be SHA256_DIGEST_LENGTH bytes or more
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 * @retval -EINVAL Do not have any xattrs for this (i.e. we should only have xattrs if we're the coordinator)
 */
int UG_inode_export_xattr_hash( struct fskit_core* fs, uint64_t gateway_id, struct UG_inode* inode, unsigned char* xattr_hash ) {

   int rc = 0;
   char** xattr_names = NULL;
   char** xattr_values = NULL;
   size_t* xattr_lengths = NULL;

   if( gateway_id != SG_manifest_get_coordinator( &inode->manifest ) ) {

       SG_error("BUG: %" PRIu64 " != %" PRIu64 "\n", gateway_id, SG_manifest_get_coordinator( &inode->manifest ) );
       exit(1);
       return -EINVAL;
   }

   rc = UG_inode_export_xattrs( fs, inode, &xattr_names, &xattr_values, &xattr_lengths );
   if( rc != 0 ) {

      SG_error("UG_inode_export_xattrs(%" PRIX64 ") rc = %d\n", UG_inode_file_id( inode ), rc );
      return rc;
   }

   if( xattr_names != NULL ) {
       /////////////////////////////////////////////////
       size_t num_xattrs = 0;
       for( num_xattrs = 0; xattr_names[num_xattrs] != NULL; num_xattrs++ );

       SG_debug("Will export hash over %zu xattrs (list follows)\n", num_xattrs);
       for( size_t i = 0; i < num_xattrs; i++ ) {
          SG_debug("   '%s'\n", xattr_names[i]);
       }
       /////////////////////////////////////////////////
   }

   memset( xattr_hash, 0, SHA256_DIGEST_LENGTH );
   rc = ms_client_xattr_hash( xattr_hash, SG_manifest_get_volume_id( &inode->manifest ), UG_inode_file_id( inode ), inode->xattr_nonce, xattr_names, xattr_values, xattr_lengths );

   SG_FREE_LIST( xattr_names, free );
   SG_FREE_LIST( xattr_values, free );
   SG_safe_free( xattr_lengths );

   return rc;
}

/**
 * @brief Export an inode to an md_entry
 * @note Does *not* set the xattr hash or signature
 * @attention src->entry must be read-locked
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 */
int UG_inode_export( struct md_entry* dest, struct UG_inode* src, uint64_t parent_id ) {

   // get type
   int type = fskit_entry_get_type( src->entry );

   memset( dest, 0, sizeof(struct md_entry) );

   if( type == FSKIT_ENTRY_TYPE_FILE ) {
      dest->type = MD_ENTRY_FILE;
   }
   else if( type == FSKIT_ENTRY_TYPE_DIR ) {
      dest->type = MD_ENTRY_DIR;
   }
   else {
      // invalid
      return -EINVAL;
   }

   char* name = SG_strdup_or_null( src->name );
   if( name == NULL ) {

      return -ENOMEM;
   }

   dest->type = type;
   dest->name = name;
   dest->file_id = fskit_entry_get_file_id( src->entry );

   fskit_entry_get_ctime( src->entry, &dest->ctime_sec, &dest->ctime_nsec );
   fskit_entry_get_mtime( src->entry, &dest->mtime_sec, &dest->mtime_nsec );

   if( type == FSKIT_ENTRY_TYPE_FILE ) {
      SG_manifest_get_modtime( &src->manifest, &dest->manifest_mtime_sec, &dest->manifest_mtime_nsec );
   }
   else {
      dest->manifest_mtime_sec = 0;
      dest->manifest_mtime_nsec = 0;
   }

   dest->write_nonce = src->write_nonce;
   dest->xattr_nonce = src->xattr_nonce;
   dest->version = SG_manifest_get_file_version( &src->manifest );
   dest->max_read_freshness = src->max_read_freshness;
   dest->max_write_freshness = src->max_write_freshness;
   dest->owner = fskit_entry_get_owner( src->entry );
   dest->coordinator = SG_manifest_get_coordinator( &src->manifest );
   dest->volume = SG_manifest_get_volume_id( &src->manifest );
   dest->mode = fskit_entry_get_mode( src->entry );
   dest->size = fskit_entry_get_size( src->entry );
   dest->error = 0;
   dest->generation = src->generation;
   dest->num_children = src->ms_num_children;
   dest->capacity = src->ms_capacity;
   dest->parent_id = parent_id;
   dest->xattr_hash = NULL;
   dest->ent_sig = NULL;
   dest->ent_sig_len = 0;

   /////////////////////////////////////

   char* tmp = NULL;
   int rc = md_entry_to_string( dest, &tmp );
   if( rc == 0 && tmp != NULL ) {
      SG_debug("Exported '%s' with:\n%s\n", dest->name, tmp );
      SG_safe_free( tmp );
   }

   /////////////////////////////////////
   return 0;
}

/**
 * @brief Check whether an exported inode's type matches the inode's type
 * @attention dest->entry must be read-locked
 * @retval 1 True
 * @retval 0 False
 */
int UG_inode_export_match_type( struct UG_inode* dest, struct md_entry* src ) {

   int type = fskit_entry_get_type( dest->entry );
   bool valid_type = false;


   if( type == FSKIT_ENTRY_TYPE_FILE && src->type == MD_ENTRY_FILE ) {

      valid_type = true;
   }
   else if( type == FSKIT_ENTRY_TYPE_DIR && src->type == MD_ENTRY_DIR ) {

      valid_type = true;
   }

   if( !valid_type ) {
      return 0;
   }
   else {
      return 1;
   }
}


/**
 * @brief Check whether an exported inode's size matches the inode's size
 * @attention dest->entry must be read-locked
 * @retval 1 True
 * @retval 0 False
 */
int UG_inode_export_match_size( struct UG_inode* dest, struct md_entry* src ) {

   // size matches?
   if( fskit_entry_get_size( dest->entry ) != (unsigned)src->size ) {

      return 0;
   }
   else {

      return 1;
   }
}


/**
 * @brief Check if an exported inode's version matches an inode's version
 * @attention dest->entry must be read-locked
 * @retval 1 True
 * @retval 0 False
 */
int UG_inode_export_match_version( struct UG_inode* dest, struct md_entry* src ) {

   // version matches?
   if( UG_inode_file_version( dest ) != src->version ) {

      return 0;
   }
   else {

      return 1;
   }
}


/**
 * @brief Check if an exported inode's file ID matches an inode's file ID
 * @attention dest->entry must be read-locked
 * @retval 1 True
 * @retval 0 False
 */
int UG_inode_export_match_file_id( struct UG_inode* dest, struct md_entry* src ) {

   // file ID matches?
   if( fskit_entry_get_file_id( dest->entry ) != src->file_id ) {

      return 0;
   }
   else {

      return 1;
   }
}


/**
 * @brief Check whether an exported inode's volume ID matches an inode's volume ID
 * @attention dest->entry must be read-locked
 * @retval 1 True
 * @retval 0 False
 */
int UG_inode_export_match_volume_id( struct UG_inode* dest, struct md_entry* src ) {

   // file ID matches?
   if( SG_manifest_get_volume_id( &dest->manifest ) != src->volume ) {

      return 0;
   }
   else {

      return 1;
   }
}


/**
 * @brief Check if an exported inode's name matches the inode's name
 * @attention dest->entry must be read-locked
 * @retval 1 True
 * @retval 0 False
 * @retval -ENOMEM Out of Memory
 */
int UG_inode_export_match_name( struct UG_inode* dest, struct md_entry* src ) {

   return (strcmp( dest->name, src->name ) == 0);
}


/**
 * @brief Import inode metadata from an md_entry
 *
 * inode must already be initialized and dest's type, file ID, version, name, and size must match src's, if dest has an associated entry.
 * The caller must make sure of this out-of-band, since changing these requires some kind of I/O or directory structure clean-up
 * @attention dest->entry must be write-locked
 * @retval 0 Success
 * @retval -EINVAL The types, IDs, versions, sizes, or names don't match (and dest has entry data)
 * @retval -EPERM dest or src or dest->entry are NULL
 */
int UG_inode_import( struct UG_inode* dest, struct md_entry* src ) {

   if( src == NULL || dest == NULL ) {

      SG_error("src == %p, dest == %p\n", src, dest );
      return -EPERM;
   }

   if( dest->entry == NULL ) {

      SG_error("dest->entry == %p\n", dest->entry );
      return -EPERM;
   }

   if( !UG_inode_export_match_volume_id( dest, src ) ) {

      SG_error("src->volume_id == %" PRIu64 ", dest->volume_id == %" PRIu64 "\n", src->volume, UG_inode_volume_id( dest ) );
      return -EINVAL;
   }

   if( !UG_inode_export_match_file_id( dest, src ) ) {

      SG_error("src->file_id == %" PRIX64 ", dest->file_id == %" PRIX64 "\n", src->file_id, UG_inode_file_id( dest ) );
      return -EINVAL;
   }

   if( UG_inode_export_match_name( dest, src ) <= 0 ) {

      SG_error("%" PRIX64 ": src->name == '%s', dest->name == '%s'\n", src->file_id, src->name, dest->name );
      return -EINVAL;
   }

   if( !UG_inode_export_match_size( dest, src ) ) {

      SG_error("%" PRIX64 ": src->size == %zu, dest->size == %zu\n", src->file_id, src->size, fskit_entry_get_size( dest->entry ) );
      return -EINVAL;
   }

   if( !UG_inode_export_match_type( dest, src ) ) {

      SG_error("%" PRIX64 ": src->type == %d, dest->type == %d\n", src->file_id, src->type, fskit_entry_get_type( dest->entry ) );
      return -EINVAL;
   }

   if( !UG_inode_export_match_version( dest, src ) ) {

      SG_error("%" PRIX64 ": src->version = %" PRId64 ", dest->version = %" PRId64 "\n", src->version, src->version, UG_inode_file_version( dest ) );
      return -EINVAL;
   }

   /////////////////////////////////////

   char* tmp = NULL;
   int rc = md_entry_to_string( src, &tmp );
   if( rc == 0 && tmp != NULL ) {
      SG_debug("Import '%s' with:\n%s\n", src->name, tmp );
      SG_safe_free( tmp );
   }

   /////////////////////////////////////

   struct timespec ts;

   // looks good!
   ts.tv_sec = src->ctime_sec;
   ts.tv_nsec = src->ctime_nsec;
   fskit_entry_set_ctime( dest->entry, &ts );

   ts.tv_sec = src->mtime_sec;
   ts.tv_nsec = src->mtime_nsec;
   fskit_entry_set_mtime( dest->entry, &ts );

   SG_manifest_set_coordinator_id( &dest->manifest, src->coordinator );
   SG_manifest_set_owner_id( &dest->manifest, src->owner );

   dest->max_read_freshness = src->max_read_freshness;
   dest->max_write_freshness = src->max_write_freshness;

   fskit_entry_set_owner_and_group( dest->entry, src->owner, src->volume );
   fskit_entry_set_mode( dest->entry, src->mode );

   dest->generation = src->generation;
   dest->ms_num_children = src->num_children;
   dest->ms_capacity = src->capacity;

   if( src->type == MD_ENTRY_FILE ) {
       dest->write_nonce = MAX( dest->write_nonce, src->write_nonce );
   }

   if( src->xattr_hash != NULL ) {
       memcpy( dest->ms_xattr_hash, src->xattr_hash, SHA256_DIGEST_LENGTH );
   }
   else {
       memset( dest->ms_xattr_hash, 0, SHA256_DIGEST_LENGTH );
   }

   return 0;
}


/**
 * @brief Create or mkdir--publish metadata, set up an fskit entry, and allocate its inode
 * @note fent will be write-locked by fskit
 * @note For files, this will disable truncate (so the subsequent trunc(2) that follows a creat(2) does not incur an extra round-trip)
 * @retval 0 Success
 * @retval -errno Failure (i.e. it exists, we don't have permission, we get a network error, etc.)
 */
int UG_inode_publish( struct SG_gateway* gateway, struct fskit_entry* fent, struct md_entry* ent_data, struct UG_inode** ret_inode_data ) {

   SG_debug("UG_inode_publish %" PRIX64 "\n", fskit_entry_get_file_id(fent) );

   int rc = 0;
   struct ms_client* ms = SG_gateway_ms( gateway );

   char const* method_name = NULL;
   bool is_mkdir = (ent_data->type == MD_ENTRY_DIR);

   // inode data
   struct UG_inode* inode = NULL;
   struct md_entry inode_data_out;

   memset( &inode_data_out, 0, sizeof(struct md_entry) );

   // make the request
   if( is_mkdir ) {

      method_name = "ms_client_mkdir";
      rc = ms_client_mkdir( ms, &inode_data_out, ent_data );
   }
   else {

      method_name = "ms_client_create";
      rc = ms_client_create( ms, &inode_data_out, ent_data );
   }

   if( rc != 0 ) {

      SG_error("%s rc = %d\n", method_name, rc );

      md_entry_free( &inode_data_out );
      return rc;
   }

   // update the child with the new inode number
   SG_debug("UG_inode_publish %" PRIX64 " renamed to %" PRIX64 "\n", fskit_entry_get_file_id( fent ), inode_data_out.file_id );
   fskit_entry_set_file_id( fent, inode_data_out.file_id );
   fskit_entry_set_mode( fent, inode_data_out.mode );
   fskit_entry_set_owner( fent, inode_data_out.owner );

   // success!  create the inode data
   inode = UG_inode_alloc( 1 );
   if( inode == NULL ) {

      return -ENOMEM;
   }

   /////////////////////////////////////

   char* tmp = NULL;
   rc = md_entry_to_string( &inode_data_out, &tmp );
   if( rc == 0 && tmp != NULL ) {
      SG_debug("Initialize inode '%s' with:\n%s\n", inode_data_out.name, tmp );
      SG_safe_free( tmp );
   }

   /////////////////////////////////////

   rc = UG_inode_init( inode, ent_data->name, fent, ms_client_get_volume_id( ms ), SG_gateway_id( gateway ), inode_data_out.version );
   if( rc != 0 ) {

      SG_error("UG_inode_init rc = %d\n", rc );

      SG_safe_free( inode );
      md_entry_free( &inode_data_out );
      return rc;
   }

   UG_inode_set_write_nonce( inode, inode_data_out.write_nonce );
   UG_inode_set_xattr_nonce( inode, inode_data_out.xattr_nonce );
   UG_inode_set_max_read_freshness( inode, inode_data_out.max_read_freshness );
   UG_inode_set_max_write_freshness( inode, inode_data_out.max_write_freshness );
   SG_manifest_set_coordinator_id( UG_inode_manifest( inode ), inode_data_out.coordinator );
   UG_inode_set_generation( inode, inode_data_out.generation );
   UG_inode_set_ms_capacity( inode, inode_data_out.capacity );
   UG_inode_set_ms_num_children( inode, inode_data_out.num_children );

   // NOTE: should be equal to file's modtime
   SG_manifest_set_modtime( UG_inode_manifest( inode ), ent_data->manifest_mtime_sec, ent_data->manifest_mtime_nsec );

   // success!
   *ret_inode_data = inode;
   md_entry_free( &inode_data_out );

   UG_inode_bind_fskit_entry( inode, fent );

   // mark as creating, so the following trunc(2) call will avoid an extra network round-trip
   UG_inode_set_creating( inode, true );

   return 0;
}


/**
 * @brief Check if an inode's manifest has a more recent modtime than the given one
 * @retval 1 True
 * @retval 0 False
 */
bool UG_inode_manifest_is_newer_than( struct SG_manifest* manifest, int64_t mtime_sec, int32_t mtime_nsec ) {

   struct timespec old_manifest_ts;
   struct timespec new_manifest_ts;

   new_manifest_ts.tv_sec = mtime_sec;
   new_manifest_ts.tv_nsec = mtime_nsec;

   old_manifest_ts.tv_sec = SG_manifest_get_modtime_sec( manifest );
   old_manifest_ts.tv_nsec = SG_manifest_get_modtime_nsec( manifest );

   bool newer = (md_timespec_diff( &new_manifest_ts, &old_manifest_ts ) > 0);

   return newer;
}


/**
 * @brief Merge new manifest block data into an inode's manifest (i.e. from reloading it remotely, or handling a remote write).
 *
 * Evict now-stale cached data and overwritten dirty blocks.
 * Remove now-invalid garbage block data.
 * @attention inode->entry must be write-locked
 * @note This method is idempotent, and will partially-succeed if it returns -ENOMEM.  Callers are encouraged to try and retry until it succeeds
 * @note This method is a commutative and associative on manifests--given manifests A, B, and C, doesn't matter what order they get merged
 * @note (i.e. merge( merge(A, B), C ) == merge( A, merge( B, C ) ) and merge( A, B ) == merge( B, A ))
 * @note Does *NOT* merge size, does *NOT* merge modtime, and does *NOT* attempt to truncate
 * @param[out] *new_manifest The new manifest
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 */
int UG_inode_manifest_merge_blocks( struct SG_gateway* gateway, struct UG_inode* inode, struct SG_manifest* new_manifest ) {

   int rc = 0;
   struct md_syndicate_cache* cache = SG_gateway_cache( gateway );

   bool replace;        // set to true if we will replace blocks in the manifest; false if not (i.e. depends on whether the "new_manifest" is actually newer)

   // i.e. if our manifest is newer than the "new" manifest, then don't replace blocks on conflict
   replace = !UG_inode_manifest_is_newer_than( UG_inode_manifest( inode ), SG_manifest_get_modtime_sec( new_manifest ), SG_manifest_get_modtime_nsec( new_manifest ) );

   // add all blocks in new_manifest
   for( SG_manifest_block_iterator itr = SG_manifest_block_iterator_begin( new_manifest ); itr != SG_manifest_block_iterator_end( new_manifest ); itr++ ) {

      uint64_t block_id = SG_manifest_block_iterator_id( itr );
      struct SG_manifest_block* new_block = SG_manifest_block_iterator_block( itr );

      UG_dirty_block_map_t::iterator dirty_block_itr;
      struct UG_dirty_block* dirty_block = NULL;

      struct SG_manifest_block* replaced_block = NULL;

      struct SG_manifest_block* existing_block = SG_manifest_block_lookup( UG_inode_manifest( inode ), block_id );
      int64_t existing_block_version = 0;

      if( existing_block != NULL ) {

         if( SG_manifest_block_version( existing_block ) == SG_manifest_block_version( new_block ) ) {

            // already merged, or no change
            continue;
         }

         // if the local block is dirty, keep the local block
         if( SG_manifest_block_is_dirty( existing_block ) ) {

            continue;
         }

         // preserve existing block data that will get overwritten
         existing_block_version = SG_manifest_block_version( existing_block );
      }

      // merge into current manifest, replacing the old one *if* the new_manifest is actually newer (makes this method commutative, associative)
      // that is, only overwrite a block if the block is not dirty, and if the new_manifest has a newer modification time (this in turn is guaranteed
      // to be monotonically increasing since there is at most one coordinator).
      rc = SG_manifest_put_block( UG_inode_manifest( inode ), new_block, replace );
      if( !replace && rc == -EEXIST ) {
         SG_debug("WARN: not replacing %" PRIu64 " (%" PRId64 " with %" PRId64 ")\n",
               SG_manifest_block_id( existing_block ), SG_manifest_block_version( existing_block ), SG_manifest_block_version( existing_block ));

         rc = 0;
         continue;
      }

      if( rc != 0 && rc != -EEXIST ) {

         break;
      }

      // clear cached block (idempotent)
      if( existing_block != NULL ) {
          md_cache_evict_block( cache, UG_inode_file_id( inode ), UG_inode_file_version( inode ), block_id, existing_block_version, 0 );
      }

      // clear dirty block (idempotent)
      dirty_block_itr = UG_inode_dirty_blocks( inode )->find( block_id );
      if( dirty_block_itr != UG_inode_dirty_blocks( inode )->end() ) {

         dirty_block = &dirty_block_itr->second;
         UG_dirty_block_evict_and_free( cache, inode, dirty_block );

         // clear invalidated garbage, if there is any (idempotent)
         replaced_block = SG_manifest_block_lookup( UG_inode_replaced_blocks( inode ), block_id );
         if( replaced_block != NULL ) {

            SG_manifest_delete_block( UG_inode_replaced_blocks( inode ), block_id );
         }
      }
   }

   return rc;
}


/**
 * @brief Remove dirty blocks from the inode
 *
 * @note The inode will no longer have modified dirty blocks
 * @attention inode->entry must be write-locked
 * @param[out] *modified Modified blocks
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 */
int UG_inode_dirty_blocks_extract( struct UG_inode* inode, UG_dirty_block_map_t* modified ) {

   for( UG_dirty_block_map_t::iterator itr = UG_inode_dirty_blocks( inode )->begin(); itr != UG_inode_dirty_blocks( inode )->end(); itr++ ) {

      // dirty?
      if( !UG_dirty_block_dirty( &itr->second ) ) {

         continue;
      }

      try {

         (*modified)[ itr->first ] = itr->second;
      }
      catch( bad_alloc& ba ) {
         return -ENOMEM;
      }
   }

   // and remove from the inode
   for( UG_dirty_block_map_t::iterator itr = modified->begin(); itr != modified->end(); itr++ ) {

      UG_dirty_block_map_t::iterator dirty_itr = UG_inode_dirty_blocks( inode )->find( itr->first );
      if( dirty_itr != UG_inode_dirty_blocks( inode )->end() ) {
         UG_inode_dirty_blocks( inode )->erase( dirty_itr );
      }
   }

   SG_debug("Extracted %zu dirty blocks (%zu remaining)\n", modified->size(), UG_inode_dirty_blocks( inode )->size() );

   return 0;
}


/**
 * @brief Return extracted dirty blocks to an inode
 *
 * Clear them out of *extracted as we do so.
 * You should call this in the same critical section as UG_inode_dirty_blocks_extract()
 * @attention inode->entry must be write-locked; locked in the same context as the _extract_ method was called
 * @note This method is idempotent.  Call it multiple times if it fails
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 */
int UG_inode_dirty_blocks_return( struct UG_inode* inode, UG_dirty_block_map_t* extracted ) {

   int rc = 0;

   for( UG_dirty_block_map_t::iterator itr = extracted->begin(); itr != extracted->end(); ) {

      try {

         (*UG_inode_dirty_blocks( inode ))[ itr->first ] = itr->second;
      }
      catch( bad_alloc& ba ) {

         rc = -ENOMEM;
         break;
      }

      UG_dirty_block_map_t::iterator old_itr = itr;
      itr++;

      extracted->erase( old_itr );
   }

   return rc;
}


/**
 * @brief Put block to an inode's dirty-block set (it can be dirty or not dirty)
 * @note Fails if there is already a block cached with a different version.
 * Succeeds if there is already a block cached, but with the same version.
 * Does not affect the inode's manifest or replaced_block sets.
 * @attention inode->entry must be write-locked
 * @note inode takes ownership of dirty_block's contents.
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 * @retval -EINVAL The block is dirty
 * @retval -EEXIST The block would replace a different block, and replace was false
 */
int UG_inode_dirty_block_put( struct SG_gateway* gateway, struct UG_inode* inode, struct UG_dirty_block* dirty_block, bool replace ) {

   struct md_syndicate_cache* cache = SG_gateway_cache( gateway );
   struct UG_dirty_block* old_dirty_block = NULL;
   UG_dirty_block_map_t::iterator old_dirty_block_itr;

   // already cached?
   old_dirty_block_itr = UG_inode_dirty_blocks( inode )->find( UG_dirty_block_id( dirty_block ) );
   if( old_dirty_block_itr != UG_inode_dirty_blocks( inode )->end() ) {

      old_dirty_block = &old_dirty_block_itr->second;

      // there's a block here. is it the same one?
      if( UG_dirty_block_version( dirty_block ) == UG_dirty_block_version( old_dirty_block ) ) {

         return 0;
      }
      else if( !replace ) {

         return -EEXIST;
      }
      else {

         //////////////////////////////////////////
         char debug_buf[52];
         memset( debug_buf, 0, 52 );
         for( unsigned int i = 0; i < (50 / 3) && i < UG_dirty_block_buf(dirty_block)->len; i++ ) {
            char nbuf[5];
            memset(nbuf, 0, 5);
            snprintf(nbuf, 4, " %02X", *(UG_dirty_block_buf(dirty_block)->data + i));
            strcat(debug_buf, nbuf);
         }
         //////////////////////////////////////////

         // evict old dirty block
         SG_debug("Replace dirty block %" PRIu64 ".%" PRId64 " with %" PRIu64 ".%" PRId64 " (%s)\n",
               UG_dirty_block_id( old_dirty_block ), UG_dirty_block_version( old_dirty_block ), UG_dirty_block_id( dirty_block ), UG_dirty_block_version( dirty_block ), debug_buf);

         UG_dirty_block_evict_and_free( cache, inode, old_dirty_block );
         UG_inode_dirty_blocks(inode)->erase( old_dirty_block_itr );

         try {
            (*UG_inode_dirty_blocks(inode))[ UG_dirty_block_id( dirty_block ) ] = *dirty_block;
         }
         catch( bad_alloc& ba ) {
            exit(1);
         }

         return 0;
      }
   }

   // not present.  put in place.
   try {

      //////////////////////////////////////////
      char debug_buf[52];
      memset( debug_buf, 0, 52 );
      for( unsigned int i = 0; i < (50 / 3) && i < UG_dirty_block_buf(dirty_block)->len; i++ ) {
         char nbuf[5];
         memset(nbuf, 0, 5);
         snprintf(nbuf, 4, " %02X", *(UG_dirty_block_buf(dirty_block)->data + i));
         strcat(debug_buf, nbuf);
      }
      //////////////////////////////////////////

      SG_debug("Insert dirty block %" PRIu64 ".%" PRId64 " (%s)\n",
            UG_dirty_block_id( dirty_block ), UG_dirty_block_version( dirty_block ), debug_buf);

      (*UG_inode_dirty_blocks( inode ))[ UG_dirty_block_id( dirty_block ) ] = *dirty_block;
   }
   catch( bad_alloc& ba ) {

      return -ENOMEM;
   }

   return 0;
}


/**
 * @brief Preserve an inode's old manifest timestamp on modification
 *
 * So we can garbage-collect the old manifest when we fsync() the inode.
 * @attention inode->entry must be write-locked
 * @return 0
 */
static int UG_inode_preserve_old_manifest_timestamp( struct UG_inode* inode ) {

   int64_t manifest_mtime_sec = 0;
   int32_t manifest_mtime_nsec = 0;
   if( SG_manifest_get_modtime_sec( &inode->replaced_blocks ) == 0 && SG_manifest_get_modtime_nsec( &inode->replaced_blocks ) == 0 ) {

      manifest_mtime_sec = SG_manifest_get_modtime_sec( &inode->manifest );
      manifest_mtime_nsec = SG_manifest_get_modtime_nsec( &inode->manifest );

      SG_manifest_set_modtime( &inode->replaced_blocks, manifest_mtime_sec, manifest_mtime_nsec );

      SG_debug("SET old manifest timestamp for %" PRIX64 " is %" PRId64 ".%d\n", UG_inode_file_id( inode ),
         SG_manifest_get_modtime_sec( &inode->replaced_blocks ), SG_manifest_get_modtime_nsec( &inode->replaced_blocks ) );

   }
   else {

      SG_debug("old manifest timestamp for %" PRIX64 " is %" PRId64 ".%d\n", UG_inode_file_id( inode ),
         SG_manifest_get_modtime_sec( &inode->replaced_blocks ), SG_manifest_get_modtime_nsec( &inode->replaced_blocks ) );

   }

   return 0;
}


/**
 * @brief Update the inode's manifest to include the dirty block info.
 *
 * The inode takes ownership of dirty_block. Remember old block information for blocks that must be garbage-collected.
 * @attention inode->entry must be write-locked!
 * @note inode takes ownership of dirty_block's contents
 * @note The block must have been flushed to disk.
 * @retval 0 Success
 * @retval 0 The block is already present in the manifest (in which case dirty_block is freed)
 * @retval -ENOMEM Out of Memory
 * @retval -EINVAL dirty_block is not dirty or not flushed
 */
int UG_inode_dirty_block_update_manifest( struct SG_gateway* gateway, struct UG_inode* inode, struct UG_dirty_block* dirty_block ) {

   int rc = 0;

   SG_debug("update manifest %" PRId64 ".%d for %" PRIX64 "[%" PRIu64 ".%" PRId64 "] (%p) (dirty=%d)\n",
            SG_manifest_get_modtime_sec( UG_inode_manifest( inode ) ), SG_manifest_get_modtime_nsec( UG_inode_manifest( inode ) ),
            UG_inode_file_id( inode ), UG_dirty_block_id( dirty_block ), UG_dirty_block_version( dirty_block ), dirty_block, UG_dirty_block_dirty(dirty_block) );

   /* DEPRECATED; don't delay writes needlessly
   // sanity check: must be flushed to disk
   if( !UG_dirty_block_is_flushed( dirty_block ) ) {

      SG_error("BUG: block [%" PRIu64 ".%" PRId64 "] is not flushed\n", UG_dirty_block_id( dirty_block ), UG_dirty_block_version( dirty_block ) );
      exit(1);
      return -EINVAL;
   }
   */

   /*
   // sanity check: must be dirty
   if( !UG_dirty_block_dirty( dirty_block ) ) {

      SG_error("BUG: block [%" PRIu64 ".%" PRId64 "] is not dirty\n", UG_dirty_block_id( dirty_block ), UG_dirty_block_version( dirty_block ) );
      exit(1);
      return -EINVAL;
   }
   */
   if( !UG_dirty_block_dirty( dirty_block ) ) {
      // nothing to do
      return 0;
   }

   struct SG_manifest_block old_block_info;

   // what blocks are being replaced?
   struct SG_manifest_block* old_block_info_ref = SG_manifest_block_lookup( UG_inode_manifest( inode ), UG_dirty_block_id( dirty_block ) );
   struct SG_manifest_block* old_replaced_block_info = SG_manifest_block_lookup( UG_inode_replaced_blocks( inode ), UG_dirty_block_id( dirty_block ) );

   // store a copy, so we can put it back on failure
   if( old_block_info_ref != NULL ) {

      rc = SG_manifest_block_dup( &old_block_info, old_block_info_ref );
      if( rc != 0 ) {

         // OOM
         return rc;
      }
   }

   // update the manifest with the new dirty block info
   rc = SG_manifest_put_block( UG_inode_manifest( inode ), UG_dirty_block_info( dirty_block ), true );
   if( rc != 0 ) {

      SG_error("SG_manifest_put_block( %" PRIX64 ".%" PRIu64" [%" PRId64 ".%" PRIu64 "] ) rc = %d\n",
               UG_inode_file_id( inode ), UG_inode_file_version( inode ), UG_dirty_block_id( dirty_block ), UG_dirty_block_version( dirty_block ), rc );

      if( old_block_info_ref != NULL ) {

         SG_manifest_block_free( &old_block_info );
      }

      return rc;
   }

   if( old_block_info_ref != NULL ) {

      if( old_replaced_block_info == NULL ) {

         // we replaced a block in the manifest, so we'll need to garbage-collect the old version.
         // we can forget about the previous old version (generated on a previous write), since it is
         // not replicated (we just need to evict any local state it had).
         rc = SG_manifest_put_block_nocopy( UG_inode_replaced_blocks( inode ), &old_block_info, true );
         if( rc != 0 ) {

            SG_error("SG_manifest_put_block( %" PRIX64 ".%" PRIu64" [%" PRId64 ".%" PRIu64 "] ) rc = %d\n",
                     UG_inode_file_id( inode ), UG_inode_file_version( inode ), old_block_info.block_id, old_block_info.block_version, rc );

            // put the old block data back
            // NOTE: guaranteed to succeed, since we're replacing without copying or allocating
            SG_manifest_put_block_nocopy( UG_inode_manifest( inode ), &old_block_info, true );

            goto UG_inode_block_update_manifest_out;
         }
      }
   }

   // this block is dirty--keep it in the face of future manifest refreshes until we replicate (via fsync())
   SG_manifest_set_block_dirty( UG_inode_manifest( inode ), UG_dirty_block_id( dirty_block ), true );
   UG_inode_preserve_old_manifest_timestamp( inode );

UG_inode_block_update_manifest_out:

   return rc;
}


/**
 * @brief Commit a single dirty block to an inode, optionally replacing an older version of the block.
 *
 * The block must have been flushed to disk.  The inode takes ownership of dirty_block.
 * Update the inode's manifest to include the dirty block info.
 * Evict the old version of the block, if it is cached.
 * Remember old block information for blocks that must be garbage-collected.
 * @attention inode->entry must be write-locked!
 * @note inode takes ownership of dirty_block's contents
 * @retval 0 Success.
 * @retval 0 The block is already present in the manifest (in which case dirty_block is freed)
 * @retval -ENOMEM Out of Memory
 * @retval -EINVAL dirty_block is not dirty or not flushed
 */
int UG_inode_dirty_block_commit( struct SG_gateway* gateway, struct UG_inode* inode, struct UG_dirty_block* dirty_block ) {

   int rc = 0;

   SG_debug("commit %" PRIX64 "[%" PRIu64 ".%" PRId64 "] (%p)\n", UG_inode_file_id( inode ), UG_dirty_block_id( dirty_block ), UG_dirty_block_version( dirty_block ), dirty_block );

   /* DEPRECATED; don't delay writes needlessly
   // sanity check: must be flushed to disk
   if( !UG_dirty_block_is_flushed( dirty_block ) ) {

      SG_error("BUG: block [%" PRIu64 ".%" PRId64 "] is not flushed\n", UG_dirty_block_id( dirty_block ), UG_dirty_block_version( dirty_block ) );
      exit(1);
      return -EINVAL;
   }
   */

   // sanity check: must be dirty
   if( !UG_dirty_block_dirty( dirty_block ) ) {

      SG_error("BUG: block [%" PRIu64 ".%" PRId64 "] is not dirty\n", UG_dirty_block_id( dirty_block ), UG_dirty_block_version( dirty_block ) );
      exit(1);
      return -EINVAL;
   }

   // update manifest...
   rc = UG_inode_dirty_block_update_manifest( gateway, inode, dirty_block );
   if( rc != 0 ) {

      SG_error("UG_inode_dirty_block_update_manifest( %" PRIX64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n",
            UG_inode_file_id( inode ), UG_dirty_block_id( dirty_block ), UG_dirty_block_version( dirty_block ), rc );

      return rc;
   }

   // put new dirty block
   rc = UG_inode_dirty_block_put( gateway, inode, dirty_block, true );
   if( rc != 0 ) {

      SG_error("FATAL: failed to put new dirty block %" PRIX64 "[%" PRIu64 ".%" PRId64 "], rc = %d\n",
            UG_inode_file_id( inode ), UG_dirty_block_id( dirty_block ), UG_dirty_block_version( dirty_block ), rc );

      exit(1);
   }

   return rc;
}


/**
 * @brief Replace the manifest of an inode
 *
 * Free the old one.
 * @attention inode->entry must be write-locked
 * @return 0
 */
int UG_inode_manifest_replace( struct UG_inode* inode, struct SG_manifest* manifest ) {

   struct SG_manifest old_manifest;

   old_manifest = inode->manifest;
   inode->manifest = *manifest;

   SG_manifest_free( &old_manifest );

   return 0;
}


/**
 * @brief Find all blocks in the inode that would be removed by a truncation
 * param[out] *removed The would-be removed blocks
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 * @attention inode->entry must be at least read-locked.
 */
int UG_inode_truncate_find_removed( struct SG_gateway* gateway, struct UG_inode* inode, off_t new_size, struct SG_manifest* removed ) {

   int rc = 0;
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t block_size = ms_client_get_volume_blocksize( ms );

   uint64_t drop_block_id = (new_size / block_size);
   if( new_size % block_size != 0 ) {
      drop_block_id ++;
   }

   uint64_t max_block_id = SG_manifest_get_block_range( UG_inode_manifest( inode ) );

   // do nothing if we're expanding
   if( UG_inode_size( inode ) <= (uint64_t)new_size ) {
      return 0;
   }

   // copy over blocks to removed
   for( uint64_t dead_block_id = drop_block_id; dead_block_id <= max_block_id; dead_block_id++ ) {

      struct SG_manifest_block* block_info = SG_manifest_block_lookup( UG_inode_manifest( inode ), dead_block_id );
      if( block_info == NULL ) {

         // write hole
         continue;
      }

      if( removed != NULL ) {

         rc = SG_manifest_put_block( removed, block_info, true );
         if( rc != 0 ) {

            // OOM
            return -ENOMEM;
         }
      }
   }

   return 0;
}


/**
 * @brief Remove all blocks beyond a given size (if there are any), and set the inode to the new size
 *
 * Drop cached blocks, drop dirty blocks, remove blocks from the manifest
 * If removed is not NULL, populate it with removed blocks
 * @attention inode->entry Must be write-locked
 * @note This method fails with -ENOMEM, and *removed is not NULL, the caller should free up *removed
 * @note new_version is 0, the version will *not* be changed
 * @param[out] new_version The new version if != 0
 * @param[out] nonce The nonce if != 0
 * @param[out] new_manifest_timestamp The new manifest timestamp if != NULL
 * @retval 0 Success
 */
int UG_inode_truncate( struct SG_gateway* gateway, struct UG_inode* inode, off_t new_size, int64_t new_version, int64_t write_nonce, struct timespec* new_manifest_timestamp ) {

   int rc = 0;
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t block_size = ms_client_get_volume_blocksize( ms );

   uint64_t drop_block_id = (new_size / block_size);
   if( new_size % block_size != 0 ) {
      drop_block_id ++;
   }

   uint64_t max_block_id = SG_manifest_get_block_range( UG_inode_manifest( inode ) );
   int64_t old_version = UG_inode_file_version( inode );
   struct md_syndicate_cache* cache = SG_gateway_cache( gateway );

   // go through the manifest and drop locally-cached blocks
   for( uint64_t dead_block_id = drop_block_id; dead_block_id <= max_block_id; dead_block_id++ ) {

      struct SG_manifest_block* block_info = SG_manifest_block_lookup( UG_inode_manifest( inode ), dead_block_id );
      if( block_info == NULL ) {

         // write hole
         continue;
      }

      // clear dirty block
      UG_dirty_block_map_t::iterator dirty_block_itr = UG_inode_dirty_blocks( inode )->find( drop_block_id );
      if( dirty_block_itr != UG_inode_dirty_blocks( inode )->end() ) {

         struct UG_dirty_block* dirty_block = &dirty_block_itr->second;

         UG_dirty_block_evict_and_free( cache, inode, dirty_block );
         UG_inode_dirty_blocks( inode )->erase( dirty_block_itr );
      }

      // clear cached block
      md_cache_evict_block( cache, UG_inode_file_id( inode ), UG_inode_file_version( inode ), dead_block_id, SG_manifest_block_version( block_info ), 0 );
   }

   if( new_version != 0 ) {

      // next version
      UG_inode_set_file_version( inode, new_version );

      // reversion only cached data
      rc = md_cache_reversion_file( cache, UG_inode_file_id( inode ), old_version, new_version, 0 );
      if( rc != 0 ) {
         SG_error("md_cache_reversion_file(%" PRIX64 " %" PRId64 " --> %" PRId64 ") rc = %d\n", UG_inode_file_id(inode), old_version, new_version, rc );
      }
   }

   // drop extra manifest blocks
   SG_manifest_truncate( UG_inode_manifest( inode ), (new_size / block_size) );

   // set new size and modtime
   UG_inode_set_size( inode, new_size );

   if( new_manifest_timestamp != NULL ) {
       SG_manifest_set_modtime( UG_inode_manifest( inode ), new_manifest_timestamp->tv_sec, new_manifest_timestamp->tv_nsec );
   }

   if( write_nonce != 0 ) {
       UG_inode_set_write_nonce( inode, write_nonce );
   }

   return 0;
}

/**
 * @brief Resolve a path to an inode and it's parent's information
 * @param[out] *parent_id The parent id
 * @return A pointer to the locked fskit_entry on success, and set *parent_id
 * @retval NULL on error, and set *rc to non-zero
 */
struct fskit_entry* UG_inode_resolve_path_and_parent( struct fskit_core* fs, char const* fs_path, bool writelock, int* rc, uint64_t* parent_id ) {

   struct resolve_parent {

      uint64_t parent_id;

      uint64_t file_id;

      // callback to fskit_entry_resolve_path_cls to remember the parent's name and ID
      // return 0 on success
      // return -ENOMEM on OOM
      static int remember_parent( struct fskit_entry* cur, void* cls ) {

         struct resolve_parent* rp = (struct resolve_parent*)cls;

          rp->parent_id = rp->file_id;

          rp->file_id = fskit_entry_get_file_id( cur );

          return 0;
      }
   };

   struct resolve_parent rp;

   struct fskit_entry* fent = fskit_entry_resolve_path_cls( fs, fs_path, 0, 0, writelock, rc, resolve_parent::remember_parent, &rp );
   if( fent == NULL ) {

      return NULL;
   }

   *parent_id = rp.parent_id;
   return fent;
}


/**
 * @brief Export an fskit_entry inode from the filesystem
 * @retval 0 Success, and fill in inode_data and *renaming from the inode
 * @retval -ENOMEM Out of Memory
 */
int UG_inode_export_fs( struct fskit_core* fs, char const* fs_path, struct md_entry* inode_data ) {

   int rc = 0;
   struct fskit_entry* fent = NULL;
   struct UG_inode* inode = NULL;

   uint64_t parent_id = 0;

   fent = UG_inode_resolve_path_and_parent( fs, fs_path, false, &rc, &parent_id );
   if( fent == NULL ) {

      return rc;
   }

   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );

   rc = UG_inode_export( inode_data, inode, parent_id );

   fskit_entry_unlock( fent );

   return rc;
}


/**
 * @brief Push a sync context to the sync queue
 * @retval 0 Success
 * @retval -ENOMEM Out of Memory
 */
int UG_inode_sync_queue_push( struct UG_inode* inode, struct UG_sync_context* sync_context ) {

   try {
      inode->sync_queue->push( sync_context );
      return 0;
   }
   catch( bad_alloc& ba ) {
      return -ENOMEM;
   }
}

/**
 * @brief Pop a sync context from the sync queue and return it
 * @retval NULL Empty
 */
struct UG_sync_context* UG_inode_sync_queue_pop( struct UG_inode* inode ) {

   struct UG_sync_context* ret = NULL;

   if( inode->sync_queue->size() > 0 ) {
      ret = inode->sync_queue->front();
      inode->sync_queue->pop();
   }

   return ret;
}

/**
 * @brief Clear the list of replaced blocks; e.g. on successful replication
 * @return 0
 */
int UG_inode_clear_replaced_blocks( struct UG_inode* inode ) {

   SG_manifest_clear( &inode->replaced_blocks );
   SG_manifest_set_modtime( &inode->replaced_blocks, 0, 0 );
   return 0;
}

// getters
/// Allocate an inode
struct UG_inode* UG_inode_alloc( int count ) {
   return SG_CALLOC( struct UG_inode, count );
}

/// Get an inode's volume ID
uint64_t UG_inode_volume_id( struct UG_inode* inode ) {
   return SG_manifest_get_volume_id( &inode->manifest );
}

/// Get an inode's coordinator ID
uint64_t UG_inode_coordinator_id( struct UG_inode* inode ) {
   return SG_manifest_get_coordinator( &inode->manifest );
}

/**
 * @brief Get the name of an inode
 *
 * @return A string copy of the inode->name
 * @retval NULL The name was not set
 * @see UG_inode_name_ref
 */
char* UG_inode_name( struct UG_inode* inode ) {
   return SG_strdup_or_null( inode->name );
}

/**
 * @brief Get the inode name
 *
 * @return The inode->name directly (not a string copy)
 * @see UG_inode_name
 */
char* UG_inode_name_ref( struct UG_inode* inode ) {
   return inode->name;
}

/// Get the inode's file ID
uint64_t UG_inode_file_id( struct UG_inode* inode ) {
   return SG_manifest_get_file_id( &inode->manifest );
}

/// Get the inode's file version
int64_t UG_inode_file_version( struct UG_inode* inode ) {
   return SG_manifest_get_file_version( &inode->manifest );
}

/// Get the inode's write_nonce
int64_t UG_inode_write_nonce( struct UG_inode* inode ) {
   return inode->write_nonce;
}

/// Get the inode's xattr_nonce
int64_t UG_inode_xattr_nonce( struct UG_inode* inode ) {
   return inode->xattr_nonce;
}

/**
 * @brief Get the inode's file size
 * @attention inode->entry must be at least read-locked
 */
uint64_t UG_inode_size( struct UG_inode* inode ) {
   // embed a sanity check (TODO: DRY)
   if( (unsigned)fskit_entry_get_size( inode->entry ) != (unsigned)SG_manifest_get_file_size( &inode->manifest ) ) {
      SG_debug("BUG: fskit entry size and manifest size mismatch (%" PRIu64 " != %" PRIu64 ")\n",
            fskit_entry_get_size(inode->entry), SG_manifest_get_file_size( &inode->manifest ) );

      exit(1);
   }

   return SG_manifest_get_file_size( &inode->manifest );
}

/// Update the inode's ms_xattr_hash
void UG_inode_ms_xattr_hash( struct UG_inode* inode, unsigned char* ms_xattr_hash ) {
   if( inode->ms_xattr_hash != NULL ) {
       memcpy( inode->ms_xattr_hash, ms_xattr_hash, SHA256_DIGEST_LENGTH );
   }
}

/// Get the inode's manifest
struct SG_manifest* UG_inode_manifest( struct UG_inode* inode ) {
   return &inode->manifest;
}

/// Get the inode's replaced_blocks
struct SG_manifest* UG_inode_replaced_blocks( struct UG_inode* inode ) {
   return &inode->replaced_blocks;
}

/// Get the inode's dirty_blocks
UG_dirty_block_map_t* UG_inode_dirty_blocks( struct UG_inode* inode ) {
   return inode->dirty_blocks;
}

/// Get the old manifest modtime (from replaced_blocks)
struct timespec UG_inode_old_manifest_modtime( struct UG_inode* inode ) {
   struct timespec ts;
   ts.tv_sec = SG_manifest_get_modtime_sec( &inode->replaced_blocks );
   ts.tv_nsec = SG_manifest_get_modtime_nsec( &inode->replaced_blocks );
   return ts;
}

/// Get the inodes fskit entry
struct fskit_entry* UG_inode_fskit_entry( struct UG_inode* inode ) {
   return inode->entry;
}

/// Check if the inode is read stale
bool UG_inode_is_read_stale( struct UG_inode* inode, struct timespec* now ) {
   if( now != NULL ) {
      return (inode->read_stale || md_timespec_diff_ms( now, &inode->refresh_time ) > inode->max_read_freshness);
   }
   else {
      return inode->read_stale;
   }
}

/// Check if the inode is write stale
bool UG_inode_is_write_stale( struct UG_inode* inode, struct timespec* now ) {
   if( inode->max_write_freshness == 0 ) {
      // always fresh
      return false;
   }

   if( now != NULL ) {
      return (md_timespec_diff_ms( now, &inode->write_refresh_time ) > inode->max_write_freshness);
   }
   else {
      return false;
   }
}

/// Check if the inode is being renamed
bool UG_inode_renaming( struct UG_inode* inode ) {
   return inode->renaming;
}

/// Check if the inode is being deleted
bool UG_inode_deleting( struct UG_inode* inode ) {
   return inode->deleting;
}

/// Check if the inode is being created
bool UG_inode_creating( struct UG_inode* inode ) {
   return inode->creating;
}

/// Get the inode's number of children
int64_t UG_inode_ms_num_children( struct UG_inode* inode ) {
   return inode->ms_num_children;
}

/// Get the maximum index number of a child
int64_t UG_inode_ms_capacity( struct UG_inode* inode ) {
   return inode->ms_capacity;
}

/// Get the time since the last read refresh
int32_t UG_inode_max_read_freshness( struct UG_inode* inode ) {
   return inode->max_read_freshness;
}

/// Get the time since the last write refresh
int32_t UG_inode_max_write_freshness( struct UG_inode* inode ) {
   return inode->max_write_freshness;
}

/// Get the last known generation number of the inode
int64_t UG_inode_generation( struct UG_inode* inode ) {
   return inode->generation;
}

/// Get the inode's refresh time
struct timespec UG_inode_refresh_time( struct UG_inode* inode ) {
   return inode->refresh_time;
}

/// Get the inode's manifest refresh time
struct timespec UG_inode_manifest_refresh_time( struct UG_inode* inode ) {
   return inode->manifest_refresh_time;
}

/// Get the time since the children were last reloaded
struct timespec UG_inode_children_refresh_time( struct UG_inode* inode ) {
   return inode->children_refresh_time;
}

/// Get the fsync queue size
size_t UG_inode_sync_queue_len( struct UG_inode* inode ) {
   return inode->sync_queue->size();
}

/// Chech whether this inode is dirty
bool UG_inode_is_dirty( struct UG_inode* inode ) {
   return inode->dirty;
}

// setters
/// Set the inode's name
int UG_inode_set_name( struct UG_inode* inode, char const* name ) {
   if( inode->name != NULL ) {
      char* name_dup = SG_strdup_or_null( name );
      if( name_dup == NULL ) {
         return -ENOMEM;
      }

      SG_safe_free( inode->name );
      inode->name = name_dup;
   }
   return 0;
}

/// Set the inode's file version
void UG_inode_set_file_version( struct UG_inode* inode, int64_t version ) {
   SG_manifest_set_file_version( &inode->manifest, version );
}

/// Set the inode's write_nonce
void UG_inode_set_write_nonce( struct UG_inode* inode, int64_t wn ) {
   inode->write_nonce = wn;
}

/// Set the inode's xattr_nonce
void UG_inode_set_xattr_nonce( struct UG_inode* inode, int64_t xn) {
   inode->xattr_nonce = xn;
}

/// Set the inode's ms_xattr_hash
void UG_inode_set_ms_xattr_hash( struct UG_inode* inode, unsigned char* ms_xattr_hash ) {
   memcpy( inode->ms_xattr_hash, ms_xattr_hash, SHA256_DIGEST_LENGTH );
}

/**
 * @brief Set the inode's refresh_time
 *
 * Called by UG_inode_set_refresh_time_now with the current time
 * @see UG_inode_set_refresh_time_now
 */
void UG_inode_set_refresh_time( struct UG_inode* inode, struct timespec* ts ) {
   inode->refresh_time = *ts;
}

/**
 * @brief Reset the inode's refresh time
 *
 * Calls UG_inode_set_refresh_time with the current time
 * @see UG_inode_set_refresh_time
 */
void UG_inode_set_refresh_time_now( struct UG_inode* inode ) {

   struct timespec now;
   clock_gettime( CLOCK_REALTIME, &now );
   UG_inode_set_refresh_time( inode, &now );
}

/**
 * @brief Set the inode's write refresh time
 *
 * Called by UG_inode_set_write_refresh_time_now with the current time
 * @see UG_inode_set_write_refresh_time_now
 */
void UG_inode_set_write_refresh_time( struct UG_inode* inode, struct timespec* ts ) {
   inode->write_refresh_time = *ts;
}

/**
 * @brief Reset the inode's write refresh time
 *
 * Calls UG_inode_set_write_refresh_time with the current time
 * @see UG_inode_set_write_refresh_time
 */
void UG_inode_set_write_refresh_time_now( struct UG_inode* inode ) {

   struct timespec now;
   clock_gettime( CLOCK_REALTIME, &now );
   UG_inode_set_write_refresh_time( inode, &now );
}

/**
 * @brief Set the inode's manifest refresh time
 *
 * Called by UG_inode_set_manifest_refresh_time_now with the current time
 * @see UG_inode_set_manifest_refresh_time_now
 */
void UG_inode_set_manifest_refresh_time( struct UG_inode* inode, struct timespec* ts ) {
   inode->manifest_refresh_time = *ts;
}

/**
 * @brief Reset the inode's manifest refresh time
 *
 * Calls UG_inode_set_manifest_refresh_time with the current time
 * @see UG_inode_set_manifest_refresh_time
 */
void UG_inode_set_manifest_refresh_time_now( struct UG_inode* inode ) {

   struct timespec now;
   clock_gettime( CLOCK_REALTIME, &now );
   UG_inode_set_manifest_refresh_time( inode, &now );
}

/**
 * @brief Set the inode's children refresh time
 *
 * Called by UG_inode_set_children_refresh_time_now with the current time
 * @see UG_inode_set_children_refresh_time_now
 */
void UG_inode_set_children_refresh_time( struct UG_inode* inode, struct timespec* ts ) {
   inode->children_refresh_time = *ts;
}

/**
 * @brief Reset the inode's children refresh time
 *
 * Calls UG_inode_set_children_refresh_time with the current time
 * @see UG_inode_set_children_refresh_time
 */
void UG_inode_set_children_refresh_time_now( struct UG_inode* inode ) {

   struct timespec now;
   clock_gettime( CLOCK_REALTIME, &now );
   UG_inode_set_children_refresh_time( inode, &now );
}

/// Set the modtime for the old manifest, replaced_blocks
void UG_inode_set_old_manifest_modtime( struct UG_inode* inode, struct timespec* ts ) {
   SG_manifest_set_modtime( &inode->replaced_blocks, ts->tv_sec, ts->tv_nsec );
}

/// Set the time (how long) since the last read refresh
void UG_inode_set_max_read_freshness( struct UG_inode* inode, int32_t rf ) {
   inode->max_read_freshness = rf;
}

/// Set the time (how long) since the last write refresh
void UG_inode_set_max_write_freshness( struct UG_inode* inode, int32_t wf ) {
   inode->max_write_freshness = wf;
}

/// Set the inode's generation number
void UG_inode_set_generation( struct UG_inode* inode, uint64_t gen ) {
   inode->generation = gen;
}

/// Set the maximum index number of a child
void UG_inode_set_ms_capacity( struct UG_inode* inode, uint64_t cap ) {
   inode->ms_capacity = cap;
}

/// Set the inode's number of children
void UG_inode_set_ms_num_children( struct UG_inode* inode, uint64_t num_children ) {
   inode->ms_num_children = num_children;
}

/// Set whether this inode shoud be revalidated before the next read
void UG_inode_set_read_stale( struct UG_inode* inode, bool val ) {
   inode->read_stale = val;
}

/// Set this inode to a "being deleted" state
void UG_inode_set_deleting( struct UG_inode* inode, bool val ) {
   inode->deleting = val;
}

/// Set this inode to a "being created" state
void UG_inode_set_creating( struct UG_inode* inode, bool val ) {
   inode->creating = val;
}

/// Set this inode to a "dirty" state
void UG_inode_set_dirty( struct UG_inode* inode, bool val ) {
   inode->dirty = val;
}

/// Set this inode's fskit entry
void UG_inode_set_fskit_entry( struct UG_inode* inode, struct fskit_entry* ent ) {
   inode->entry = ent;
}

/**
 * @brief Set the size associated with this inode
 * @attention Requires inode->entry to be write-locked
 * @note The entry can sometimes be NULL, such as when we're initializing the root inode
 */
void UG_inode_set_size( struct UG_inode* inode, uint64_t new_size ) {
   if( inode->entry != NULL ) {
       fskit_entry_set_size( inode->entry, new_size );
   }

   SG_manifest_set_size( &inode->manifest, new_size );
}

/**
 * @brief Bind an inode and fskit_entry
 *
 * Attach an fskit_entry to an inode, and the inode to the fskit_entry
 * @attention ent must be write-locked
 */
void UG_inode_bind_fskit_entry( struct UG_inode* inode, struct fskit_entry* ent ) {
   UG_inode_set_fskit_entry( inode, ent );
   fskit_entry_set_user_data( ent, inode );
}


/**
 * @brief Preserve the old manifest timestamp
 *
 * If it's not set, then copy it from the current manifest
 * @note Requires exclusive access to inode
 */
void UG_inode_preserve_old_manifest_modtime( struct UG_inode* inode ) {

   if( SG_manifest_get_modtime_sec( &inode->replaced_blocks ) == 0 && SG_manifest_get_modtime_nsec( &inode->replaced_blocks ) == 0 ) {

      struct timespec ts;
      ts.tv_sec = SG_manifest_get_modtime_sec( UG_inode_manifest( inode ) );
      ts.tv_nsec = SG_manifest_get_modtime_nsec( UG_inode_manifest( inode ) );

      SG_debug("Old manifest timestamp of %" PRIX64 " is %d.%d\n", UG_inode_file_id( inode ), (int)ts.tv_sec, (int)ts.tv_nsec );

      UG_inode_set_old_manifest_modtime( inode, &ts );
   }
}


/**
 * @brief Get the number of non-dirty (clean) blocks there are in the inode's dirty block set
 *
 * @attention inode->entry must be read-locked
 * @note These blocks get cached here on read
 */
uint64_t UG_inode_count_clean_blocks( struct UG_inode* inode ) {

    uint64_t count = 0;
    for( UG_dirty_block_map_t::iterator itr = inode->dirty_blocks->begin(); itr != inode->dirty_blocks->end(); itr++ ) {
        if( !UG_dirty_block_dirty( &itr->second ) ) {
           count += 1;
        }
    }

    return count;
}


/**
 * @brief Get the block ID of the nth non-dirty block
 * @retval -1 Failure
 * @attention inode->entry must be read-locked
 */
uint64_t UG_inode_find_clean_block_id( struct UG_inode* inode, uint64_t n ) {

   uint64_t block_id = -1;
   uint64_t i = n;
   for( UG_dirty_block_map_t::iterator itr = inode->dirty_blocks->begin(); itr != inode->dirty_blocks->end(); itr++ ) {
       if( !UG_dirty_block_dirty( &itr->second ) ) {
            i--;
            if( i == 0 ) {
               block_id = itr->first;
               break;
            }
       }
   }

   return block_id;
}


/**
 * @brief Evict a clean block from the dirty inode set
 * @retval 0 Success
 * @retval -ENOENT Absent
 * @retval -EINVAL Dirty
 * @attention inode->entry must be write-locked
 */
int UG_inode_evict_clean_block( struct UG_inode* inode, uint64_t block_id ) {

    UG_dirty_block_map_t::iterator itr = inode->dirty_blocks->find(block_id);
    if( itr == inode->dirty_blocks->end() ) {
       return -ENOENT;
    }

    if( UG_dirty_block_dirty( &itr->second ) ) {
       return -EINVAL;
    }

    struct UG_dirty_block* dirty_block = &itr->second;
    inode->dirty_blocks->erase( itr );
    UG_dirty_block_free( dirty_block );
    return 0;
}

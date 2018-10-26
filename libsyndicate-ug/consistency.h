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
 * @file libsyndicate-ug/consistency.h
 * @author Jude Nelson
 * @date 9 Mar 2016
 *
 * @brief Header file for consistency.cpp related functions
 *
 * @note On loading a manifest, set the inode's old manifest timestamp to it, so we can vacuum it later.

 * @see libsyndicate-ug/consistency.cpp
 */

#ifndef _UG_CONSISTENCY_H_
#define _UG_CONSISTENCY_H_

#include <libsyndicate/libsyndicate.h>
#include <libsyndicate/ms/ms-client.h>
#include <libsyndicate/ms/path.h>
#include <libsyndicate/ms/getattr.h>
#include <libsyndicate/ms/listdir.h>

#include <fskit/fskit.h>

extern "C" {

// go fetch an inode directoy from the MS
int UG_consistency_inode_download( struct SG_gateway* gateway, uint64_t file_id, struct md_entry* ent );

// get the manifest from one of a list of gateways
int UG_consistency_manifest_download( struct SG_gateway* gateway, struct SG_request_data* reqdat, uint64_t coordinator_id, uint64_t* gateway_ids, size_t num_gateway_ids, struct SG_manifest* manifest, struct md_download_connection_pool* dlcpool );

// reload the path's-worth of metadata
int UG_consistency_path_ensure_fresh( struct SG_gateway* gateway, char const* fs_path );

// reload a directory's children
int UG_consistency_dir_ensure_fresh( struct SG_gateway* gateway, char const* fs_path );

// reload an inode's manifest
int UG_consistency_manifest_ensure_fresh( struct SG_gateway* gateway, char const* fs_path );
int UG_consistency_manifest_ensure_fresh2( struct SG_gateway* gateway, char const* fs_path, struct md_download_connection_pool* dlcpool );

// fetch the xattrs for an inode
int UG_consistency_fetchxattrs( struct SG_gateway* gateway, uint64_t file_id, int64_t xattr_nonce, unsigned char* xattr_hash, fskit_xattr_set** ret_xattrs );

// ensure a locally-cached inode is fresh
int UG_consistency_inode_ensure_fresh( struct SG_gateway* gateway, char const* fs_path, struct UG_inode* inode );
int UG_consistency_inode_ensure_fresh_ex( struct SG_gateway* gateway, char const* fs_path, struct UG_inode* inode, bool locked, struct fskit_entry* dent );

// ask a remote gateway to refresh its inode
int UG_consistency_request_refresh( struct SG_gateway* gateway, char const* path );

}

#endif

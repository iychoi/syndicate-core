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
 * @file libsyndicate/drivers/coblitz/coblitz.h
 * @author Jude Nelson
 * @date 9 Mar 2016
 *
 * @brief Header file for coblitz.cpp
 *
 * @see libsyndicate/drivers/coblitz/coblitz.cpp
 */

#ifndef _DRIVER_COBLITZ_H_
#define _DRIVER_COBLITZ_H_

#include "libsyndicate/closure.h"
#include "libsyndicate/download.h"

/// Coblitz cls structure
struct coblitz_cls {
   char* cdn_prefix; ///< CDN prefix
};

extern "C" {

int closure_init( struct md_closure* closure, void** cls );
int closure_shutdown( void* cls );
int connect_cache( struct md_closure* closure, CURL* curl, char const* url, void* cls );

}

#endif 
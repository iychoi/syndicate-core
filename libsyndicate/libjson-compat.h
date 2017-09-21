/*
   Copyright 2013 The Trustees of Princeton University

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
 * @file libsyndicate/libjson-compat.h
 * @author Jude Nelson
 * @date 9 Mar 2016
 *
 * @brief Include the proper libjson headers
 *
 * Under Debian include json-c/json.h, otherwise include json/json.h 
 */

#ifdef _DISTRO_DEBIAN 
#include <json-c/json.h>

#else
#include <json/json.h>

#endif
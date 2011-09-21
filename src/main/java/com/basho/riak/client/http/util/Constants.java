/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.basho.riak.client.http.util;

public interface Constants {

    // Default URL path prefixes Riak HTTP interface
    public static String RIAK_URL_PREFIX = "/riak";

    // JSON fields used by Riak
    public static String FL_NAME = "name";
    public static String FL_KEYS = "keys";
    public static String FL_SCHEMA = "props";
    public static String FL_SCHEMA_ALLOW_MULT = "allow_mult";
    public static String FL_SCHEMA_CHASHFUN = "chash_keyfun";
    public static String FL_SCHEMA_CHASHFUN_MOD = "mod";
    public static String FL_SCHEMA_CHASHFUN_FUN = "fun";
    public static String FL_SCHEMA_LINKFUN = "linkfun";
    public static String FL_SCHEMA_LINKFUN_MOD = "mod";
    public static String FL_SCHEMA_LINKFUN_FUN = "fun";
    public static String FL_SCHEMA_NVAL = "n_val";
    public static String FL_SCHEMA_FUN_MOD = "mod";
    public static String FL_SCHEMA_FUN_FUN = "fun";
    public static String FL_SCHEMA_FUN_NAME = "name";
    public static String FL_SCHEMA_LAST_WRITE_WINS = "last_write_wins";
    public static String FL_SCHEMA_BACKEND = "backend";
    public static String FL_SCHEMA_SMALL_VCLOCK = "small_vclock";
    public static String FL_SCHEMA_BIG_VCLOCK = "big_vclock";
    public static String FL_SCHEMA_YOUNG_VCLOCK = "young_vclock";
    public static String FL_SCHEMA_OLD_VCLOCK = "old_vclock";
    public static String FL_SCHEMA_R = "r";
    public static String FL_SCHEMA_W = "w";
    public static String FL_SCHEMA_DW = "dw";
    public static String FL_SCHEMA_RW = "rw";
    public static String FL_SCHEMA_POSTCOMMIT = "postcommit";
    public static String FL_SCHEMA_PRECOMMIT = "precommit";
    public static String FL_SCHEMA_SEARCH = "search";
    public static String FL_BUCKETS = "buckets";

    // Header directives used by Riak
    public static String LINK_TAG = "riaktag";

    // HTTP headers used in Riak
    public static String HDR_ACCEPT = "accept";
    public static String HDR_CLIENT_ID = "x-riak-clientid";
    public static String HDR_CONNECTION = "connection";
    public static String HDR_CONTENT_LENGTH = "content-length";
    public static String HDR_CONTENT_TYPE = "content-type";
    public static String HDR_ETAG = "etag";
    public static String HDR_IF_MATCH = "if-match";
    public static String HDR_IF_MODIFIED_SINCE = "if-modified-since";
    public static String HDR_IF_UNMODIFIED_SINCE = "if-unmodified-since";
    public static String HDR_IF_NONE_MATCH = "if-none-match";
    public static String HDR_LAST_MODIFIED = "last-modified";
    public static String HDR_LINK = "link";
    public static String HDR_LOCATION = "location";
    public static String HDR_VCLOCK = "x-riak-vclock";
    // Declared twice because of Erlang has bizarre HTTP header case handling.
    // If a header name is 21 chars or shorteer, it is auto-capitalized between
    // dashes. Otherwise, it is passed as is. Therefore, we just make sure this
    // headers prefix is correctly capitalized in requests.
    public static String HDR_USERMETA_PREFIX = "x-riak-meta-";
    public static String HDR_USERMETA_REQ_PREFIX = "X-Riak-Meta-";
    public static String HDR_SEC_INDEX_REQ_PREFIX = "X-Riak-Index-";
    public static String HDR_SEC_INDEX_PREFIX = "x-riak-index-";

    // Content types used in Riak
    public static String CTYPE_ANY = "*/*";
    public static String CTYPE_JSON = "application/json";
    public static String CTYPE_JSON_UTF8 = "application/json;charset=UTF-8";
    public static String CTYPE_OCTET_STREAM = "application/octet-stream";
    public static String CTYPE_MULTIPART_MIXED = "multipart/mixed";
    public static String CTYPE_TEXT = "text/plain";
    public static String CTYPE_TEXT_UTF8 = "text/plain; charset=UTF-8";

    // Default r, w, and dw values to use when not specified
    public static Integer DEFAULT_R = 2;
    public static Integer DEFAULT_W = null;
    public static Integer DEFAULT_DW = null;
    
    // Values for the "keys" query parameter
    public static String NO_KEYS = "false";
    public static String INCLUDE_KEYS = "true";
    public static String STREAM_KEYS = "stream";

    // Query parameters used in Riak
    public static String QP_RETURN_BODY = "returnbody";
    public static String QP_R = "r";
    public static String QP_W = "w";
    public static String QP_DW = "dw";
    public static String QP_RW = "rw";
    public static String QP_KEYS = "keys";
    public static String QP_BUCKETS = "buckets";

    // HTTP method names
    public static String HTTP_HEAD_METHOD = "HEAD";
    public static String HTTP_GET_METHOD = "GET";
    public static String HTTP_PUT_METHOD = "PUT";
    public static String HTTP_DELETE_METHOD = "DELETE";

    // Riak magic numbers
    public static int RIAK_CLIENT_ID_LENGTH = 4;

    // List bucket operation parameters
    public static String LIST_BUCKETS = "true";
}

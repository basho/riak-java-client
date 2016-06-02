/*
 * Copyright 2013 Basho Technologies Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basho.riak.client.core.util;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 1.0
 */
public interface Constants
{
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
    public static String FL_SCHEMA_PR = "pr";
    public static String FL_SCHEMA_PW = "pw";
    public static String FL_SCHEMA_BASIC_QUORUM = "basic_quorum";
    public static String FL_SCHEMA_NOT_FOUND_OK = "notfound_ok";
    public static String FL_SCHEMA_POSTCOMMIT = "postcommit";
    public static String FL_SCHEMA_PRECOMMIT = "precommit";
    public static String FL_SCHEMA_SEARCH = "search";
    public static String FL_BUCKETS = "buckets";

    // Header directives used by Riak
    public static String LINK_TAG = "riaktag";

    // Content types used in Riak
    public static String CTYPE_ANY = "*/*";
    public static String CTYPE_JSON = "application/json";
    public static String CTYPE_JSON_UTF8 = "application/json; charset=UTF-8";
    public static String CTYPE_OCTET_STREAM = "application/octet-stream";
    public static String CTYPE_MULTIPART_MIXED = "multipart/mixed";
    public static String CTYPE_TEXT = "text/plain";
    public static String CTYPE_TEXT_UTF8 = "text/plain; charset=UTF-8";

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
    public static String QP_PR = "pr";
    public static String QP_PW = "pw";
    public static String QP_NOT_FOUND_OK = "notfound_ok";
    public static String QP_BASIC_QUORUM = "basic_quorum";

    // List bucket operation parameters
    public static String LIST_BUCKETS = "true";
    
    // Netty Channel handler constants
    public static final String MESSAGE_CODEC = "codec";
    public static final String OPERATION_ENCODER = "operationEncoder";
    public static final String RESPONSE_HANDLER = "responseHandler";
    public static final String SSL_HANDLER = "sslHandler";
    public static final String HEALTHCHECK_CODEC = "healthCheckCodec";

    public static final String CLIENT_OPTION_CHARSET = "com.basho.riak.client.DefaultCharset";
}

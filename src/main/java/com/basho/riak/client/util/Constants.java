/*
This file is provided to you under the Apache License,
Version 2.0 (the "License"); you may not use this file
except in compliance with the License.  You may obtain
a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.  
*/
package com.basho.riak.client.util;

public interface Constants {

    public static String JIAK_PREFIX = "/jiak";
    public static String RAW_PREFIX = "/raw";
    
    public static Integer DEFAULT_R = 2; 
    public static Integer DEFAULT_W = null; 
    public static Integer DEFAULT_DW = null;

    public static int DEFAULT_STREAM_BUFFER_SIZE = 4096;

    public static String JIAK_FL_BUCKET = "bucket";
    public static String JIAK_FL_KEY = "key";
    public static String JIAK_FL_KEYS = "keys";
    public static String JIAK_FL_LAST_MODIFIED = "lastmod";
    public static String JIAK_FL_LINKS = "links";
    public static String JIAK_FL_SCHEMA = "schema";
    public static String JIAK_FL_VALUE = "object";
    public static String JIAK_FL_VCLOCK = "vclock";
    public static String JIAK_FL_VTAG = "vtag";
    public static String JIAK_FL_USERMETA = "usermeta";
    public static String JIAK_FL_WALK_RESULTS = "results";
    
    public static String RAW_FL_PROPS = "props";
    public static String RAW_FL_KEYS = "keys";

    public static String RAW_LINK_TAG = "riaktag";
    
    public static String HDR_ACCEPT = "accept";
    public static String HDR_CONTENT_TYPE = "content-type";
    public static String HDR_ETAG = "etag";
    public static String HDR_LAST_MODIFIED = "last-modified";
    public static String HDR_LINK = "link";
    public static String HDR_USERMETA_PREFIX = "x-riak-meta-";
    public static String HDR_VCLOCK = "x-riak-vclock";
    
    public static String CTYPE_ANY = "*/*";
    public static String CTYPE_JSON = "application/json";
    public static String CTYPE_OCTET_STREAM = "application/octet-stream";
    public static String CTYPE_MULTIPART_MIXED = "multipart/mixed";
    public static String CTYPE_TEXT = "text/plain";
    
    public static String QP_RETURN_BODY = "returnbody";
    public static String QP_R = "r";
    public static String QP_W = "w";
    public static String QP_DW = "dw";
}

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
package com.basho.riak.client;

import java.io.IOException;
import java.util.List;

import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.request.RiakWalkSpec;
import com.basho.riak.client.response.BucketResponse;
import com.basho.riak.client.response.FetchResponse;
import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.StoreResponse;
import com.basho.riak.client.response.StreamHandler;
import com.basho.riak.client.response.WalkResponse;

public interface RiakClient {

    public HttpResponse setBucketSchema(String bucket,
            List<String> allowedFields, List<String> writeMask,
            List<String> readMask, List<String> requiredFields,
            RequestMeta meta);
    public HttpResponse setBucketSchema(String bucket,
            List<String> allowedFields, List<String> writeMask,
            List<String> readMask, List<String> requiredFields);

    public BucketResponse listBucket(String bucket, RequestMeta meta);
    public BucketResponse listBucket(String bucket);
            
    public StoreResponse store(RiakObject object, RequestMeta meta);
    public StoreResponse store(RiakObject object);

    public FetchResponse fetchMeta(String bucket, String key, RequestMeta meta);
    public FetchResponse fetchMeta(String bucket, String key);

    public FetchResponse fetch(String bucket, String key, RequestMeta meta);
    public FetchResponse fetch(String bucket, String key);

    public boolean stream(String bucket, String key, StreamHandler handler, RequestMeta meta) throws IOException;

    public HttpResponse delete(String bucket, String key, RequestMeta meta);
    public HttpResponse delete(String bucket, String key);

    public WalkResponse walk(String bucket, String key, String walkSpec, RequestMeta meta);
    public WalkResponse walk(String bucket, String key, String walkSpec);
    public WalkResponse walk(String bucket, String key, RiakWalkSpec walkSpec);

}
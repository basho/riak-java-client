/*
 * Copyright 2014 Brian Roach <roach at basho dot com>.
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

package com.basho.riak.client.api.commands.itest;

import com.basho.riak.client.api.annotations.RiakBucketName;
import com.basho.riak.client.api.annotations.RiakBucketType;
import com.basho.riak.client.api.annotations.RiakContentType;
import com.basho.riak.client.api.annotations.RiakIndex;
import com.basho.riak.client.api.annotations.RiakKey;
import com.basho.riak.client.api.annotations.RiakLastModified;
import com.basho.riak.client.api.annotations.RiakTombstone;
import com.basho.riak.client.api.annotations.RiakVClock;
import com.basho.riak.client.api.annotations.RiakVTag;
import com.basho.riak.client.api.cap.VClock;
import java.util.Set;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class RiakAnnotatedPojo
{
    @RiakKey
    public String key;

    @RiakBucketName
    public String bucketName;

    @RiakBucketType
    public String bucketType;

    @RiakVClock
    public VClock vclock;

    @RiakContentType
    public String contentType;

    @RiakLastModified
    public Long lastModified;

    @RiakVTag
    public String vtag;

    @RiakTombstone
    public boolean deleted;
    
    @RiakIndex(name="email")
    public Set<String> emailIndx;
    
    @RiakIndex(name="user_id")
    public Long userId;
    
    public String value;
    
    

}

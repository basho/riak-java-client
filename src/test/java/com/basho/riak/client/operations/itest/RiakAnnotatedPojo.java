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

package com.basho.riak.client.operations.itest;

import com.basho.riak.client.annotations.RiakBucketName;
import com.basho.riak.client.annotations.RiakBucketType;
import com.basho.riak.client.annotations.RiakContentType;
import com.basho.riak.client.annotations.RiakKey;
import com.basho.riak.client.annotations.RiakLastModified;
import com.basho.riak.client.annotations.RiakTombstone;
import com.basho.riak.client.annotations.RiakVClock;
import com.basho.riak.client.annotations.RiakVTag;
import com.basho.riak.client.cap.VClock;

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
    
    public String value;

}

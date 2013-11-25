/*
 * Copyright 2013 Basho Technologies Inc
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
package com.basho.riak.client.operations;

import com.basho.riak.client.util.ByteArrayWrapper;

public class Location
{

    private static final String DEFAULT_TYPE = "default";
    private static final BucketType DEFAULT_BUCKET_TYPE = new BucketType(DEFAULT_TYPE);

    private final ByteArrayWrapper type;
    private final ByteArrayWrapper bucket;
    private final ByteArrayWrapper key;

    protected Location(ByteArrayWrapper type, ByteArrayWrapper bucket, ByteArrayWrapper key)
    {
        this.type = type;
        this.bucket = bucket;
        this.key = key;
    }

    protected Location(String type, String bucket, String key)
    {
        this.type = ByteArrayWrapper.create(type);
        this.bucket = ByteArrayWrapper.create(bucket);
        this.key = key == null ? null : ByteArrayWrapper.create(key);
    }

    public static BucketType defaultBucketType()
    {
        return DEFAULT_BUCKET_TYPE;
    }

    public static BucketType bucketType(ByteArrayWrapper type)
    {
        return new BucketType(type);
    }

    public static BucketType bucketType(String type)
    {
        return new BucketType(type);
    }

    public static Bucket bucket(ByteArrayWrapper type, ByteArrayWrapper bucket)
    {
        return new Bucket(type, bucket);
    }

    public static Bucket bucket(BucketType type, ByteArrayWrapper bucket)
    {
        return new Bucket(type.getType(), bucket);
    }

    public static Bucket bucket(String type, String bucket)
    {
        return new Bucket(type, bucket);
    }

    public static Bucket bucket(BucketType type, String bucket)
    {
        return new Bucket(type.getType(), ByteArrayWrapper.create(bucket));
    }

    public static Bucket bucket(ByteArrayWrapper bucket)
    {
        ByteArrayWrapper type = ByteArrayWrapper.create(DEFAULT_TYPE);
        return new Bucket(type, bucket);
    }

    public static Bucket bucket(String bucket)
    {
        return new Bucket(DEFAULT_TYPE, bucket);
    }

    public static Key key(ByteArrayWrapper type, ByteArrayWrapper bucket, ByteArrayWrapper key)
    {
        return new Key(type, bucket, key);
    }

    public static Key key(String type, String bucket, String key)
    {
        return new Key(type, bucket, key);
    }

    public static Key key(ByteArrayWrapper bucket, ByteArrayWrapper key)
    {
        ByteArrayWrapper type = ByteArrayWrapper.create(DEFAULT_TYPE);
        return new Key(type, bucket, key);
    }

    public static Key key(String bucket, String key)
    {
        return new Key(DEFAULT_TYPE, bucket, key);
    }

    public static Key key(Bucket bucket, ByteArrayWrapper key)
    {
        return new Key(bucket.getType(), bucket.getBucket(), key);
    }

    public static Key key(Bucket bucket, String key)
    {
        return new Key(bucket.getType(), bucket.getBucket(), ByteArrayWrapper.create(key));
    }

    public ByteArrayWrapper getType()
    {
        return type;
    }

    public ByteArrayWrapper getBucket()
    {
        return bucket;
    }

    public boolean hasKey()
    {
        return key != null;
    }

    public ByteArrayWrapper getKey()
    {
        return key;
    }

    @Override
    public int hashCode()
    {
        return super.hashCode();  //TODO
    }

    @Override
    public boolean equals(Object obj)
    {
        return super.equals(obj); //TODO
    }

    @Override
    public String toString()
    {
        return "{type: " + type + ", bucket: " + bucket + ", key: " + key + "}";
    }
}

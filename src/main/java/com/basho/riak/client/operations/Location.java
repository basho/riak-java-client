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

public abstract class Location
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

    public ByteArrayWrapper getType()
    {
        return type;
    }

    public ByteArrayWrapper getBucket()
    {
        return bucket;
    }

    public boolean hasType()
    {
        return type != null;
    }

    public boolean hasKey()
    {
        return key != null;
    }

    public boolean hasBucket()
    {
        return bucket != null;
    }

    public ByteArrayWrapper getKey()
    {
        return key;
    }

    @Override
    public int hashCode()
    {
        int result = 17;
        result = 37 * result + getType().hashCode();
        result = 37 * result + getBucket().hashCode();
        result = 37 * result + getKey().hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }

        if (!(obj instanceof Location))
        {
            return false;
        }

        Location other = (Location) obj;

        return ((!hasType() && !other.hasType()) || (hasType() && other.hasType() && getType().equals(other.getType()))) &&
            ((!hasBucket() && !other.hasBucket()) && (hasBucket() && other.hasBucket() && getBucket().equals(other.getBucket()))) &&
            ((!hasKey() && !other.hasKey()) || (hasKey() && other.hasKey() && getKey().equals(other.getKey())));
    }

    @Override
    public String toString()
    {
        return "{type: " + type + ", bucket: " + bucket + ", key: " + key + "}";
    }
}

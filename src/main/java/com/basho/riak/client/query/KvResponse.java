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
package com.basho.riak.client.query;

import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.util.ByteArrayWrapper;

/**
 * A wrapper around a content from Riak.
 * <p/>
 * The key, bucket, and bucket type form a coordinate as to where data lives in
 * Riak rather than being part of that data. Additional metatdata such at the
 * vector clock apply to the response. This allows us to model that properly.
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class KvResponse<T> extends RiakResponse
{

    private final T content;
    private final boolean notFound;
    private final boolean unchanged;
    private final VClock vclock;

    private KvResponse(Builder<T> builder)
    {
        super(builder.bucketName, builder.key, builder.bucketType);
        this.content = builder.content;
        this.notFound = builder.notFound;
        this.unchanged = builder.unchanged;
        this.vclock = builder.vclock;
    }

    /**
     * Determine whether any content was returned.
     *
     * @return true if there is content in this res nponse.
     */
    public boolean hasContent()
    {
        return content != null;
    }

    /**
     * Return the content of this response.
     *
     * @return the content or null if none was returned.
     */
    public T getContent()
    {
        return content;
    }

    /**
     * Determine if a vector clock is present.
     *
     * @return true if present, false otherwise.
     */
    public boolean hasVClock()
    {
        return vclock != null;
    }

    /**
     * Return the vector clock returned in this response.
     *
     * @return the vector clock, null if not present.
     */
    public VClock getVClock()
    {
        return vclock;
    }

    /**
     * Determine whether an object was found.
     *
     * @return true if found and returned, false otherwise.
     */
    public boolean notFound()
    {
        return notFound;
    }

    /**
     * Determine if a conditional query returned unchanged.
     *
     * @return True if the object was unchanged, false otherwise.
     */
    public boolean unchanged()
    {
        return unchanged;
    }

    public static class Builder<T>
    {

        private T content;
        private boolean notFound;
        private boolean unchanged;
        private VClock vclock;

        private ByteArrayWrapper key;
        private ByteArrayWrapper bucketName;
        private ByteArrayWrapper bucketType =
            ByteArrayWrapper.unsafeCreate("default".getBytes());

        public Builder(ByteArrayWrapper bucketName, ByteArrayWrapper key)
        {
            if (null == bucketName || bucketName.length() == 0 || null == key || key.length() == 0)
            {
                throw new IllegalArgumentException("Bucket name and key cannot be null or zero length");
            }
            this.bucketName = bucketName;
            this.key = key;
        }

        public Builder withBucketType(ByteArrayWrapper bucketType)
        {
            if (bucketType != null)
            {
                if (bucketType.length() == 0)
                {
                    throw new IllegalArgumentException("Bucket type cannot be null or zero length");
                }
                else
                {
                    this.bucketType = bucketType;
                }
            }
            return this;
        }

        public Builder<T> withContent(T content)
        {
            this.content = content;
            return this;
        }

        public Builder<T> withNotFound(boolean notFound)
        {
            this.notFound = notFound;
            return this;
        }

        public Builder<T> withUnchanged(boolean unchanged)
        {
            this.unchanged = unchanged;
            return this;
        }

        public Builder<T> withVClock(VClock vclock)
        {
            this.vclock = vclock;
            return this;
        }

        public KvResponse<T> build()
        {
            return new KvResponse<T>(this);
        }

    }
}

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
package com.basho.riak.client.query;

import com.basho.riak.client.query.crdt.types.CrdtElement;
import com.basho.riak.client.util.ByteArrayWrapper;

public class CrdtResponse extends RiakResponse
{

    private final ByteArrayWrapper context;
    private final CrdtElement crdtElement;

    private CrdtResponse(Builder builder)
    {
        super(builder.bucketName, builder.key, builder.bucketType);
        this.context = builder.context;
        this.crdtElement = builder.crdtElement;
    }

    public boolean hasContext()
    {
        return context != null;
    }

    public ByteArrayWrapper getContext()
    {
        return context;
    }

    public boolean hasCrdtElement()
    {
        return crdtElement != null;
    }

    public CrdtElement getCrdtElement()
    {
        return crdtElement;
    }

    public static class Builder
    {

        private final ByteArrayWrapper bucketName;
        private final ByteArrayWrapper key;
        private ByteArrayWrapper bucketType =
            ByteArrayWrapper.create("default");
        private ByteArrayWrapper context;
        private CrdtElement crdtElement;

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

        public Builder withContext(ByteArrayWrapper context)
        {
            if (context != null)
            {
                if (context.length() == 0)
                {
                    throw new IllegalArgumentException("Context cannot be null or zero length");
                }
                else
                {
                    this.context = context;
                }
            }
            return this;
        }

        public Builder withCrdtElement(CrdtElement crdtElement)
        {
            this.crdtElement = crdtElement;
            return this;
        }

        public CrdtResponse build()
        {
            return new CrdtResponse(this);
        }

    }

}

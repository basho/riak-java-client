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

package com.basho.riak.client.api.commands.buckets;

import com.basho.riak.client.api.AsIsRiakCommand;
import com.basho.riak.client.core.operations.FetchBucketTypePropsOperation;
import com.basho.riak.client.core.util.BinaryValue;

import java.util.Objects;

/**
 * Command used to fetch the properties of a bucket type in Riak.
 * <script src="https://google-code-prettify.googlecode.com/svn/loader/run_prettify.js"></script>
 * <p>
 * <pre class="prettyprint">
 * {@code
 * FetchBucketTypeProperties fbp = new FetchBucketTypeProperties.Builder("myBucketType").build();
 * FetchBucketTypePropsOperation.Response resp = client.execute(fbp);
 * BucketProperties props = resp.getProperties();}</pre>
 * Note that this simply returns the core response
 * {@link com.basho.riak.client.core.operations.FetchBucketTypePropsOperation.Response}
 * <p>
 * </p>
 *
 * @author Luke Bakken <lbakken@basho.com>
 * @since 2.2
 */
public final class FetchBucketTypeProperties extends AsIsRiakCommand<FetchBucketTypePropsOperation.Response, BinaryValue>
{
    private final BinaryValue bucketType;

    public FetchBucketTypeProperties(Builder builder)
    {
        this.bucketType = builder.bucketType;
    }

    @Override
    protected FetchBucketTypePropsOperation buildCoreOperation()
    {
        return new FetchBucketTypePropsOperation.Builder(bucketType).build();
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
        {
            return true;
        }

        if (!(other instanceof FetchBucketTypeProperties))
        {
            return false;
        }

        FetchBucketTypeProperties otherFetchBucketProperties = (FetchBucketTypeProperties)other;

        return Objects.equals(bucketType, otherFetchBucketProperties.bucketType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bucketType);
    }

    /**
     * Builder used to construct a FetchBucketPoperties command.
     */
    public static class Builder
    {
        private final BinaryValue bucketType;

        /**
         * Construct a Builder for a FetchBucketTypeProperties command.
         *
         * @param bucketType The bucket type name.
         */
        public Builder(String bucketType)
        {
            if (bucketType == null)
            {
                throw new IllegalArgumentException("Bucket type cannot be null");
            }
            this.bucketType = BinaryValue.create(bucketType);
        }

        /**
         * Construct a Builder for a FetchBucketTypeProperties command.
         *
         * @param bucketType The bucket type name.
         */
        public Builder(BinaryValue bucketType)
        {
            if (bucketType == null)
            {
                throw new IllegalArgumentException("Bucket type cannot be null");
            }
            this.bucketType = bucketType;
        }

        /**
         * Construct a new FetchBucketTypeProperties command.
         *
         * @return a new FetchBucketTypeProperties command.
         */
        public FetchBucketTypeProperties build()
        {
            return new FetchBucketTypeProperties(this);
        }
    }
}

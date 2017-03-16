/*
 * Copyright Basho Technologies Inc
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

import com.basho.riak.client.core.operations.StoreBucketTypePropsOperation;
import com.basho.riak.client.core.util.BinaryValue;


/**
 * Command used to store (modify) the properties of a bucket type in Riak.
 * <p>
 * <pre class="prettyprint">
 * {@code
 * StoreBucketTypeProperties sbp = new StoreBucketTypeProperties.Builder("myBucketType")
 * .withAllowMulti(true)
 * .build();
 * client.execute(sbp);}</pre>
 * </p>
 *
 * @author Luke Bakken <lbakken@basho.com>
 * @since 2.2
 */
public final class StoreBucketTypeProperties extends StoreProperties<BinaryValue>
{
    StoreBucketTypeProperties(BinaryValue bucketType, Builder builder)
    {
        super(bucketType, builder);
    }

    @Override
    protected StoreBucketTypePropsOperation buildCoreOperation()
    {
        StoreBucketTypePropsOperation.Builder builder = new StoreBucketTypePropsOperation.Builder(bucketOrType);
        populatePropertiesOperation(builder);
        return builder.build();
    }

    public static class Builder extends StoreProperties.PropsBuilder<Builder>
    {
        private final BinaryValue bucketType;

        /**
         * Create a new Builder from a bucket type name as BinaryValue
         * @param bucketType
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
         * Create a new Builder from a bucket type name as String
         * @param bucketType
         */
        public Builder(String bucketType)
        {
            if (bucketType == null)
            {
                throw new IllegalArgumentException("Bucket type cannot be null");
            }
            this.bucketType = BinaryValue.create(bucketType);
        }

        public StoreBucketTypeProperties build()
        {
            return new StoreBucketTypeProperties(bucketType, this);
        }

        @Override
        protected Builder self()
        {
            return this;
        }
    }
}

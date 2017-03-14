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

import com.basho.riak.client.core.operations.StoreBucketPropsOperation;
import com.basho.riak.client.core.query.Namespace;

/**
 * Command used to store (modify) the properties of a bucket in Riak.
 * <p>
 * <pre class="prettyprint">
 * {@code
 * Namespace ns = new Namespace("my_type", "my_bucket");
 * StoreBucketProperties sbp =
 * new StoreBucketProperties.Builder(ns)
 * .withAllowMulti(true)
 * .build();
 * client.execute(sbp);}</pre>
 * </p>
 *
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public final class StoreBucketProperties extends StoreProperties<Namespace>
{
    StoreBucketProperties(Namespace namespace, Builder builder)
    {
        super(namespace, builder);
    }

    @Override
    protected StoreBucketPropsOperation buildCoreOperation()
    {
        StoreBucketPropsOperation.Builder builder = new StoreBucketPropsOperation.Builder(bucketOrType);
        populatePropertiesOperation(builder);
        return builder.build();
    }

    public static class Builder extends StoreProperties.PropsBuilder<Builder>
    {
        private final Namespace namespace;

        /**
         * Create a new Builder from a Namespace
         * @param namespace
         */
        public Builder(Namespace namespace)
        {
            if (namespace == null)
            {
                throw new IllegalArgumentException("Namespace cannot be null");
            }
            this.namespace = namespace;
        }

        public StoreBucketProperties build()
        {
            return new StoreBucketProperties(namespace, this);
        }

        @Override
        protected Builder self()
        {
            return this;
        }
    }
}

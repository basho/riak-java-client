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
import com.basho.riak.client.core.operations.FetchBucketPropsOperation;
import com.basho.riak.client.core.query.Namespace;

/**
 * Command used to fetch the properties of a bucket in Riak.
 * <script src="https://google-code-prettify.googlecode.com/svn/loader/run_prettify.js"></script>
 * <p>
 * <pre class="prettyprint">
 * {@code
 * Namespace ns = new Namespace("my_type", "my_bucket");
 * FetchBucketProperties fbp = new FetchBucketProperties.Builder(ns).build();
 * FetchBucketPropsOperation.Response resp = client.execute(fbp);
 * BucketProperties props = resp.getBucketProperties();}</pre>
 * Note that this simply returns the core response {@link com.basho.riak.client.core.operations.FetchBucketPropsOperation.Response}
 *
 * </p>
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public final class FetchBucketProperties extends AsIsRiakCommand<FetchBucketPropsOperation.Response, Namespace>
{
    private final Namespace namespace;

    public FetchBucketProperties(Builder builder)
    {
        this.namespace = builder.namespace;
    }

    @Override
    protected FetchBucketPropsOperation buildCoreOperation()
    {
        return new FetchBucketPropsOperation.Builder(namespace).build();
    }

    /**
     * Builder used to construct a FetchBucketPoperties command.
     */
    public static class Builder
    {
        private final Namespace namespace;

        /**
         * Construct a Builder for a FetchBucketProperties command.
         * @param namespace The namespace for the bucket.
         */
        public Builder(Namespace namespace)
        {
            if (namespace == null)
            {
                throw new IllegalArgumentException("Namespace cannot be null");
            }
            this.namespace = namespace;
        }

        /**
         * Construct a new FetchBucketProperties command.
         * @return a new FetchBucketProperties command.
         */
        public FetchBucketProperties build()
        {
            return new FetchBucketProperties(this);
        }
    }
}

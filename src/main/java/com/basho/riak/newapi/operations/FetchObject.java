/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.basho.riak.newapi.operations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import com.basho.riak.client.raw.Command;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakResponse;
import com.basho.riak.newapi.RiakObject;
import com.basho.riak.newapi.RiakRetryFailedException;
import com.basho.riak.newapi.bucket.Bucket;
import com.basho.riak.newapi.cap.ConflictResolver;
import com.basho.riak.newapi.cap.DefaultRetrier;
import com.basho.riak.newapi.cap.UnresolvedConflictException;
import com.basho.riak.newapi.convert.ConversionException;
import com.basho.riak.newapi.convert.Converter;

/**
 * @author russell
 * 
 */
public class FetchObject<T> implements RiakOperation<T> {

    private final Bucket bucket;
    private final RawClient client;
    private final String key;

    private int retries = 0;

    private Integer r;
    private ConflictResolver<T> resolver;
    private Converter<T> converter;

    /**
     * @param bucket
     * @param client
     */
    public FetchObject(final RawClient client, final Bucket bucket, final String key) {
        this.bucket = bucket;
        this.client = client;
        this.key = key;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.RiakOperation#execute()
     */
    public T execute() throws UnresolvedConflictException, RiakRetryFailedException, ConversionException {
        // fetch, resolve
        Command<RiakResponse> command = new Command<RiakResponse>() {
            public RiakResponse execute() throws IOException {
                if (r != null) {
                    return client.fetch(bucket, key, r);
                } else {
                    return client.fetch(bucket, key);
                }
            }
        };

        final RiakResponse ros = new DefaultRetrier().attempt(command, retries);
        final Collection<T> siblings = new ArrayList<T>(ros.numberOfValues());

        for (RiakObject o : ros) {
            siblings.add(converter.toDomain(o));
        }

        return resolver.resolve(siblings);
    }

    public FetchObject<T> withResolver(ConflictResolver<T> resolver) {
        this.resolver = resolver;
        return this;
    }

    public FetchObject<T> r(int r) {
        this.r = r;
        return this;
    }

    public FetchObject<T> withConverter(Converter<T> converter) {
        this.converter = converter;
        return this;
    }

    public FetchObject<T> retry(int times) {
        this.retries = times;
        return this;
    }
}

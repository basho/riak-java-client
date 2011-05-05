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
package com.basho.riak.client.operations;

import java.util.concurrent.Callable;

import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.cap.Retrier;
import com.basho.riak.client.raw.RawClient;

/**
 * @author russell
 * 
 */
public class DeleteObject implements RiakOperation<Void> {

    private final RawClient client;
    private final String bucket;
    private final String key;

    private Retrier retrier;

    private Integer rw;

    /**
     * @param client
     * @param bucket
     * @param key
     */
    public DeleteObject(RawClient client, String bucket, String key, final Retrier retrier) {
        this.client = client;
        this.bucket = bucket;
        this.key = key;
        this.retrier = retrier;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.RiakOperation#execute()
     */
    public Void execute() throws RiakRetryFailedException {
        Callable<Void> command = new Callable<Void>() {
            public Void call() throws Exception {
                if (rw == null) {
                    client.delete(bucket, key);
                } else {
                    client.delete(bucket, key, rw);
                }
                return null;
            }
        };

       retrier.attempt(command);
        return null;
    }

    public DeleteObject rw(Integer rw) {
        this.rw = rw;
        return this;
    }

    public DeleteObject retrier(final Retrier retrier) {
        this.retrier = retrier;
        return this;
    }
}

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

import com.basho.riak.client.raw.Command;
import com.basho.riak.client.raw.DefaultRetrier;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.newapi.RiakRetryFailedException;
import com.basho.riak.newapi.bucket.Bucket;

/**
 * @author russell
 * 
 */
public class DeleteObject implements RiakOperation<Void> {

    private final RawClient client;
    private final Bucket bucket;
    private final String key;

    private Integer rw;
    private int retries = 0;

    /**
     * @param client
     * @param bucket
     * @param key
     */
    public DeleteObject(RawClient client, Bucket bucket, String key) {
        this.client = client;
        this.bucket = bucket;
        this.key = key;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.RiakOperation#execute()
     */
    public Void execute() throws RiakRetryFailedException {
        Command<Void> command = new Command<Void>() {
            public Void execute() throws IOException {
                if (rw == null) {
                    client.delete(bucket, key);
                } else {
                    client.delete(bucket, key, rw);
                }
                return null;
            }
        };

        new DefaultRetrier().attempt(command, retries);
        return null;
    }

    public DeleteObject rw(int rw) {
        this.rw = rw;
        return this;
    }

    public DeleteObject retry(int times) {
        this.retries = times;
        return this;
    }

}

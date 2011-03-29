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

import com.basho.riak.newapi.RiakRetryFailedException;

/**
 * @author russell
 * 
 */
public class DeleteObject<T> implements RiakOperation<T> {

    private Integer rw;
    private int retries = 0;

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.RiakOperation#execute()
     */
    public T execute() throws RiakRetryFailedException {
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

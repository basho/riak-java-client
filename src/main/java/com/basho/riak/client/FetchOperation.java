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
package com.basho.riak.client;

import java.util.Arrays;

/**
 * @author russell
 *
 */
public class FetchOperation implements RiakOperation<RiakObject> {
    
    private Integer r;
    private ConflictResolver resolver = new DoNothingResolver();

    /* (non-Javadoc)
     * @see com.basho.riak.client.RiakOperation#execute()
     */
    public RiakObject execute() throws UnresolvedConflictException, RiakRetryFailedException {
        return resolver.resolve(Arrays.asList(new RiakObject[] {}));
    }

    public FetchOperation withResolver(ConflictResolver resolver) {
        this.resolver = resolver;
        return this;
    }

    public FetchOperation r(int r) {
        this.r = r;
        return this;
    }

}

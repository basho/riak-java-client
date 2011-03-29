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
import com.basho.riak.newapi.cap.ConflictResolver;
import com.basho.riak.newapi.cap.UnresolvedConflictException;
import com.basho.riak.newapi.convert.Converter;

/**
 * @author russell
 *
 */
public class FetchObject<T> implements RiakOperation<T> {
    
    private Integer r;
    private ConflictResolver<T> resolver;
    private Converter<T> converter;

    /* (non-Javadoc)
     * @see com.basho.riak.client.RiakOperation#execute()
     */
    public T execute() throws UnresolvedConflictException, RiakRetryFailedException {
        return null;
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

}

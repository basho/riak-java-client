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
package com.basho.riak.newapi.cap;

import java.util.Collection;

import com.basho.riak.newapi.RiakException;

/**
 * @author russell
 * 
 */
public class UnresolvedConflictException extends RiakException {

    /**
     * eclipse generated id
     */
    private static final long serialVersionUID = -219858468775752064L;

    private final String reason;
    private final Collection<? extends Object> siblings;

    public UnresolvedConflictException(String reason, Collection<? extends Object> siblings) {
        this.reason = reason;
        this.siblings = siblings;
    }

    /**
     * @return the reason
     */
    public String getReason() {
        return reason;
    }

    /**
     * @param <T>
     * @return the siblings
     */
    public Collection<? extends Object> getSiblings() {
        return siblings;
    }

}

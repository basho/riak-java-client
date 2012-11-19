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
package com.basho.riak.pbc;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/**
 * Encapsulates greater detail about the result of a fetch.
 * 
 * @author russell
 * 
 */
@Immutable
public class FetchResponse {

    private final RiakObject[] objects;
    private final boolean unchanged;
    private final byte[] vclock;

    /**
     * @param objects
     *            the set of riak sibling values
     * @param unchanged
     *            is this an 'unchanged' response to a conditional fetch
     * @param vclock
     *            the vclock in the response (if any)
     */
    protected FetchResponse(RiakObject[] objects, boolean unchanged, @Nullable byte[] vclock) {
        this.objects = objects;
        this.unchanged = unchanged;
        if(vclock != null) {
            this.vclock = vclock.clone();
        } else {
            this.vclock = null;
        }
    }

    /**
     * @return the objects
     */
    public RiakObject[] getObjects() {
        return objects.clone();
    }

    /**
     * @return true if Riak PB API returned unchanged in response to a
     *         conditional fetch, false otherwise
     */
    public boolean isUnchanged() {
        return unchanged;
    }

    /**
     * @return the vclock (if one is present) from the response.
     */
    @Nullable
    public byte[] getVClock() {
        if (this.vclock != null) {
            return this.vclock.clone();
        }
        
        return null;
    }

    /**
     * @return true if there are sibling values in this fetch response, false
     *         otherwise
     */
    public boolean hasSiblings() {
        return objects.length > 1;
    }
}

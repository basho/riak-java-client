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
package com.basho.riak.client.raw;

import java.util.Arrays;
import java.util.Iterator;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.cap.BasicVClock;
import com.basho.riak.client.cap.VClock;

/**
 * A data response from Riak separated into a single vector clock and an array of sibling values.
 * 
 * Immutable
 * 
 * @author russell
 */
public class RiakResponse implements Iterable<IRiakObject> {

    private static final IRiakObject[] NO_OBJECTS = new IRiakObject[] {};
    private final VClock vclock;
    private final IRiakObject[] riakObjects;

    /**
     * Create a response from the given vector clock and array. Array maybe
     * empty or null as may the vector clock (which implies no data, right.)
     * 
     * @param vclock
     * @param riakObjects
     */
    public RiakResponse(byte[] vclock, IRiakObject[] riakObjects) {
        if (vclock == null) {
            this.vclock = null;
        } else {
            this.vclock = new BasicVClock(vclock);
        }
        if (riakObjects == null) {
            this.riakObjects = NO_OBJECTS;
        } else {
            this.riakObjects = riakObjects;
        }
    }

    /**
     * Create THE empty response
     */
    private RiakResponse() {
        this.riakObjects = NO_OBJECTS;
        this.vclock = null;
    }

    /**
     * Get the vector clock bytes
     * @return the vclock
     */
    public byte[] getVclockBytes() {
        return vclock.getBytes();
    }

    /**
     * Get the vector clock as a {@link VClock}
     * @return the vector clock
     */
    public VClock getVclock() {
        return vclock;
    }

    /**
     * Gets the actual array of {@link IRiakObject} (not a clone or copy, so treat it well)
     * @return the riakObjects
     */
    public IRiakObject[] getRiakObjects() {
        return riakObjects;
    }

    /**
     * Does the response have sibling values?
     * @return
     */
    public boolean hasSiblings() {
        return riakObjects.length > 1;
    }

    /**
     * Does the response hav *any* values?
     * @return
     */
    public boolean hasValue() {
        return riakObjects.length > 0;
    }

    /**
     * How many values?
     * @return
     */
    public int numberOfValues() {
        return riakObjects.length;
    }

    /**
     * Unmodifiable iterator view of the values returned from Riak.
     */
    public Iterator<IRiakObject> iterator() {
        return Arrays.asList(riakObjects).iterator();
    }

    /**
     * Generate the empty response
     * @return THE empty response
     */
    public static RiakResponse empty() {
        return new RiakResponse();
    }
}

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

import com.basho.riak.newapi.IRiakObject;
import com.basho.riak.newapi.cap.BasicVClock;
import com.basho.riak.newapi.cap.VClock;

/**
 * What riak returns: a VClock and bunch of siblings.
 * 
 * @author russell
 */
public class RiakResponse implements Iterable<IRiakObject> {

    private static final IRiakObject[] NO_OBJECTS = new IRiakObject[] {};
    private final VClock vclock;
    private final IRiakObject[] riakObjects;

    /**
     * @param vclock
     * @param riakObjects
     */
    public RiakResponse(byte[] vclock, IRiakObject[] riakObjects) {
        this.vclock = new BasicVClock(vclock);
        if (riakObjects == null) {
            this.riakObjects = NO_OBJECTS;
        } else {
            this.riakObjects = riakObjects;
        }
    }

    /**
     * 
     */
    private RiakResponse() {
        this.riakObjects = NO_OBJECTS;
        this.vclock = null;
    }

    /**
     * @return the vclock
     */
    public byte[] getVclockBytes() {
        return vclock.getBytes();
    }

    /**
     * @return the vclock
     */
    public VClock getVclock() {
        return vclock;
    }

    /**
     * @return the riakObjects
     */
    public IRiakObject[] getRiakObjects() {
        return riakObjects;
    }

    public boolean hasSiblings() {
        return riakObjects.length > 1;
    }

    public boolean hasValue() {
        return riakObjects.length > 0;
    }

    public int numberOfValues() {
        return riakObjects.length;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Iterable#iterator()
     */
    public Iterator<IRiakObject> iterator() {
        return Arrays.asList(riakObjects).iterator();
    }

    /**
     * @return
     */
    public static RiakResponse empty() {
        return new RiakResponse();
    }

}

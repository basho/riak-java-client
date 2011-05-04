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
package com.basho.riak.client.raw.query;

import java.util.Iterator;
import java.util.LinkedList;

import com.basho.riak.client.query.LinkWalkStep;
import com.basho.riak.client.util.UnmodifiableIterator;

/**
 * An immutable class that represents a link walk specification
 *
 * @author russell
 * 
 */
public class LinkWalkSpec implements Iterable<LinkWalkStep> {

    private final LinkedList<LinkWalkStep> steps;
    private final String startBucket;
    private final String startKey;

    /**
     * @param steps
     * @param startBucket
     * @param startKey
     */
    public LinkWalkSpec(final LinkedList<LinkWalkStep> steps, String startBucket, String startKey) {
        this.steps = new LinkedList<LinkWalkStep>(steps);
        this.startBucket = startBucket;
        this.startKey = startKey;
    }

    /**
     * @return the startBucket
     */
    public String getStartBucket() {
        return startBucket;
    }

    /**
     * @return the startKey
     */
    public String getStartKey() {
        return startKey;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Iterable#iterator()
     */
    public Iterator<LinkWalkStep> iterator() {
        final Iterator<LinkWalkStep> it = steps.iterator();
        return new UnmodifiableIterator<LinkWalkStep>(it);
    }

    /**
     * @return how many steps in this link spec
     */
    public int size() {
        return steps.size();
    }
}

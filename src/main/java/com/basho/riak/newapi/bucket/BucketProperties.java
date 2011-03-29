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
package com.basho.riak.newapi.bucket;

import java.util.Collection;

import com.basho.riak.newapi.cap.Quorum;
import com.basho.riak.newapi.query.NamedErlangFunction;
import com.basho.riak.newapi.query.NamedFunction;

/**
 * @author russell
 *
 */
public interface BucketProperties {

    
    /**
     * @return the allowSiblings if set, or null if not
     */
    Boolean getAllowSiblings();

    /**
     * @return the lastWriteWins if set or null if not
     */
    Boolean getLastWriteWins();

    /**
     * @return the nVal if set or null if not
     */
    Integer getNVal();

    /**
     * @return the backend if set, or null.
     */
    String getBackend();

    /**
     * 
     * @return the small vclock pruning property if set, or null.
     */
    int getSmallVClock();

    /**
     * 
     * @return the big vclock pruning size property if set, or null.
     */
    int getBigVClock();

    /**
     * 
     * @return the young vclock prune property if set, or null.
     */
    long getYoungVClock();

    /**
     * 
     * @return the old vclock prune property if set, or null
     */
    long getOldVClock();

    /**
     * @return the pre commit hooks, if any, or an empty collection.
     */
    Collection<NamedFunction> getPrecommitHooks();

    /**
     * @return the post commit hooks, if ant, or an empty collection.
     */
    Collection<NamedErlangFunction> getPostcommitHooks();

    /**
     * 
     * @return the default CAP read quorum for this bucket, or null.
     */
    Quorum getR();

    /**
     * 
     * @return the default CAP write quorum for this bucket, or null.
     */
    Quorum getW();

    /**
     * 
     * @return the default CAP RW (delete) quorum for this bucket, or null.
     */
    Quorum getRW();

    /**
     * 
     * @return the default CAP durable write quorum for this bucket, or null.
     */
    Quorum getDW();

    /**
     * @return the key hashing function for the bucket, or null.
     */
    NamedErlangFunction getChashKeyFunction();

    /**
     * @return the link walking function for the bucket, or null.
     */
    NamedErlangFunction getLinkWalkFunction();

}
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
package com.basho.riak.client.bucket;

import java.util.Collection;

import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.query.functions.NamedErlangFunction;
import com.basho.riak.client.query.functions.NamedFunction;

/**
 * The set of properties for a bucket, things like n_val, allow_mult, default
 * read quorum.
 * 
 * <p>
 * Depending on what the underlying transport exposes some of these values will
 * be null. I'm working on (a) updating the low-level clients to return all
 * available values and (b) updating Riak's protocol buffers interface to return
 * all the bucket properties.
 * </p>
 * 
 * @author russell
 * 
 */
public interface BucketProperties {

    /**
     * The allow_mult value for the bucket.
     * 
     * @return the allowSiblings if set, or null if not
     */
    Boolean getAllowSiblings();

    /**
     * The last_write_wins value for the bucket.
     * 
     * @return the lastWriteWins if set or null if not
     */
    Boolean getLastWriteWins();

    /**
     * This bucket's n_val.
     * 
     * @return the nVal if set or null if not
     */
    Integer getNVal();

    /**
     * The backend used by this bucket.
     * 
     * @return the backend if set, or null.
     */
    String getBackend();

    /**
     * the small_vclock property for this bucket. See <a href=
     * "https://help.basho.com/entries/448442-how-does-riak-control-the-growth-of-vector-clocks"
     * >controlling vector clock growth</a> for details.
     * 
     * @return the small vector clock pruning property if set, or null.
     */
    Integer getSmallVClock();

    /**
     * the big_vclock property for this bucket. See <a href=
     * "https://help.basho.com/entries/448442-how-does-riak-control-the-growth-of-vector-clocks"
     * >controlling vector clock growth</a> for details.
     * 
     * @return the big vclock pruning size property if set, or null.
     */
    Integer getBigVClock();

    /**
     * The young_vclcok property for this bucket. See <a href=
     * "https://help.basho.com/entries/448442-how-does-riak-control-the-growth-of-vector-clocks"
     * >controlling vector clock growth</a> for details.
     * 
     * @return the young vclock prune property if set, or null.
     */
    Long getYoungVClock();

    /**
     * the old_vclock property for this bucket. See <a href=
     * "https://help.basho.com/entries/448442-how-does-riak-control-the-growth-of-vector-clocks"
     * >controlling vector clock growth</a> for details.
     * 
     * @return the old vclock prune property if set, or null
     */
    Long getOldVClock();

    /**
     * The set of precommit_hooks for this bucket. See <a
     * href="http://wiki.basho.com/Pre--and-Post-Commit-Hooks.html">pre and post
     * commit hooks</a> for more details.
     * 
     * @return the precommit hooks, if any, or an empty collection.
     */
    Collection<NamedFunction> getPrecommitHooks();

    /**
     * The set of postcommit hooks for this bucket. See <a
     * href="http://wiki.basho.com/Pre--and-Post-Commit-Hooks.html">pre and post
     * commit hooks</a> for more details.
     * 
     * @return the post commit hooks, if ant, or an empty collection.
     */
    Collection<NamedErlangFunction> getPostcommitHooks();

    /**
     * The default <code>r</code> quorum for this bucket.
     * 
     * @return the default CAP read quorum for this bucket, or null.
     */
    Quorum getR();

    /**
     * The default <code>w</code> quorum for this bucket.
     * 
     * @return the default CAP write quorum for this bucket, or null.
     */
    Quorum getW();

    /**
     * The default <code>rw</code> quorum for this bucket.
     * 
     * @return the default CAP RW (delete) quorum for this bucket, or null.
     */
    Quorum getRW();

    /**
     * The default <code>dw</code> quorum for this bucket.
     * 
     * @return the default CAP durable write quorum for this bucket, or null.
     */
    Quorum getDW();

    /**
     * The chash_keyfun for this bucket.
     * 
     * @return the key hashing function for the bucket, or null.
     */
    NamedErlangFunction getChashKeyFunction();

    /**
     * The linkwalk_fun for this bucket.
     * 
     * @return the link walking function for the bucket, or null.
     */
    NamedErlangFunction getLinkWalkFunction();

}
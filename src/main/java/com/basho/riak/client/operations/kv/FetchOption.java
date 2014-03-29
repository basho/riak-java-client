/*
 * Copyright 2013 Basho Technologies Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basho.riak.client.operations.kv;

import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.operations.RiakOption;

/**
 * Options for controlling how Riak performs the fetch operation.
 * <p>
 * These options can be supplied to the {@link FetchValue.Builder} to change
 * how Riak performs the operation. These override the defaults provided
 * by the bucket.
 * </p>
 *
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 * @see <a href="http://docs.basho.com/riak/latest/dev/advanced/cap-controls/">Replication Properties</a>
 */
public final class FetchOption<T> extends RiakOption<T>
{

    /**
     * Read Quorum.
     * How many replicas need to agree when fetching the object.
     */
    public static final FetchOption<Quorum> R = new FetchOption<Quorum>("R");
    /**
     * Primary Read Quorum.
     * How many primary replicas need to be available when retrieving the object.
     */
    public static final FetchOption<Quorum> PR = new FetchOption<Quorum>("PR");
    /**
     * Basic Quorum.
     * Whether to return early in some failure cases (eg. when r=1 and you get 
     * 2 errors and a success basic_quorum=true would return an error)
     */
    public static final FetchOption<Boolean> BASIC_QUORUM = new FetchOption<Boolean>("BASIC_QUORUM");
    /**
     * Not Found OK.
     * Whether to treat notfounds as successful reads for the purposes of R
     */
    public static final FetchOption<Boolean> NOTFOUND_OK = new FetchOption<Boolean>("NOTFOUND_OK");
    /**
     * If Modified.
     * When a vector clock is supplied with this option, only return the object 
     * if the vector clocks don't match.
     */
    public static final FetchOption<VClock> IF_MODIFIED = new FetchOption<VClock>("IF_MODIFIED");
    /**
     * Head.
     * return the object with the value(s) set as empty. This allows you to get the 
     * meta data without a potentially large value. Analogous to an HTTP HEAD request.
     */
    public static final FetchOption<Boolean> HEAD = new FetchOption<Boolean>("HEAD");
    /**
     * Deleted VClock.
     * By default single tombstones are not returned by a fetch operations. This 
     * will return a Tombstone if it is present. 
     */
    public static final FetchOption<Boolean> DELETED_VCLOCK = new FetchOption<Boolean>("DELETED_VCLOCK");
    /**
     * Timeout.
     * Sets the server-side timeout for this operation. The default in Riak is 60 seconds.
     */
    public static final FetchOption<Integer> TIMEOUT = new FetchOption<Integer>("TIMEOUT");
    public static final FetchOption<Boolean> SLOPPY_QUORUM = new FetchOption<Boolean>("SLOPPY_QUORUM");
    public static final FetchOption<Integer> N_VAL = new FetchOption<Integer>("N_VAL");

    private FetchOption(String name)
    {
        super(name);
    }

}

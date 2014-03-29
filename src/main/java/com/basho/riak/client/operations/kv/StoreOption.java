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
import com.basho.riak.client.operations.RiakOption;

/**
 * Options For controlling how Riak performs the store operation.
 * <p>
 * These options can be supplied to the {@link StoreValue.Builder} to change
 * how Riak performs the operation. These override the defaults provided
 * by the bucket.
 * </p>
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 * @see <a href="http://docs.basho.com/riak/latest/dev/advanced/cap-controls/">Replication Properties</a>
 */
public final class StoreOption<T> extends RiakOption<T>
{

    /**
     * Write Quorum.
     * How many replicas to write to before returning a successful response.
     */
    public static final StoreOption<Quorum> W = new StoreOption<Quorum>("W");
    /**
     * Durable Write Quorum.
     * How many replicas to commit to durable storage before returning a successful response.
     */
    public static final StoreOption<Quorum> DW = new StoreOption<Quorum>("DW");
    /**
     * Primary Write Quorum.
     * How many primary nodes must be up when the write is attempted.
     */
    public static final StoreOption<Quorum> PW = new StoreOption<Quorum>("PW");
    /**
     * If Not Modified.
     * Update the value only if the vclock in the supplied object matches the one in the database.
     */
    public static final StoreOption<Boolean> IF_NOT_MODIFIED = new StoreOption<Boolean>("IF_NOT_MODIFIED");
    /**
     * If None Match.
     * Store the value only if this bucket/key combination are not already defined.
     */
    public static final StoreOption<Boolean> IF_NONE_MATCH = new StoreOption<Boolean>("IF_NONE_MATCH");
    /**
     * Return Body.
     * Return the object stored in Riak. Note this will return all siblings.
     */
    public static final StoreOption<Boolean> RETURN_BODY = new StoreOption<Boolean>("RETURN_BODY");
    /**
     * Return Head.
     * Like {@link #RETURN_BODY} except that the value(s) in the object are blank to 
     * avoid returning potentially large value(s).
     */
    public static final StoreOption<Boolean> RETURN_HEAD = new StoreOption<Boolean>("RETURN_HEAD");
    /**
     * Timeout.
     * Sets the server-side timeout for this operation. The default in Riak is 60 seconds.
     */
    public static final StoreOption<Integer> TIMEOUT = new StoreOption<Integer>("TIMEOUT");
    
    public static final StoreOption<Boolean> ASIS = new StoreOption<Boolean>("ASIS");
    public static final StoreOption<Boolean> SLOPPY_QUORUM = new StoreOption<Boolean>("SLOPPY_QUORUM");
    public static final StoreOption<Integer> N_VAL = new StoreOption<Integer>("N_VAL");
    

    private StoreOption(String name)
    {
        super(name);
    }
}

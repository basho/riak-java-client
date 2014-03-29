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
 * Options for controlling how Riak performs the delete operation. 
 * <p>
 * These options can be supplied to the {@link DeleteValue.Builder} to change
 * how Riak performs the operation. These override the defaults provided
 * by the bucket.
 * </p>
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 * @see <a href="http://docs.basho.com/riak/latest/dev/advanced/cap-controls/">Replication Properties</a>
 */
public class DeleteOption<T> extends RiakOption<T>
{

    /**
     * Read Write Quorum.
     * Quorum for both operations (get and put) involved in deleting an object 
     */
    public static final DeleteOption<Quorum> RW = new DeleteOption<Quorum>("RW");
    /**
     * Read Quorum.
     * How many replicas need to agree when fetching the object.
     */
    public static final DeleteOption<Quorum> R = new DeleteOption<Quorum>("R");
    /**
     * Write Quorum.
     * How many replicas to write to before returning a successful response.
     */
    public static final DeleteOption<Quorum> W = new DeleteOption<Quorum>("W");
    /**
     * Primary Read Quorum.
     * How many primary replicas need to be available when retrieving the object.
     */
    public static final DeleteOption<Quorum> PR = new DeleteOption<Quorum>("PR");
    /**
     * Primary Write Quorum. 
     * How many primary nodes must be up when the write is attempted
     */
    public static final DeleteOption<Quorum> PW = new DeleteOption<Quorum>("PW");
    /**
     * Durable Write Quorum.
     * How many replicas to commit to durable storage before returning a successful response.
     */
    public static final DeleteOption<Quorum> DW = new DeleteOption<Quorum>("DW");
    /**
     * Timeout.
     * Sets the server-side timeout for this operation. The default is 60 seconds.
     */
    public static final DeleteOption<Integer> TIMEOUT = new DeleteOption<Integer>("TIMEOUT");
    public static final DeleteOption<Boolean> SLOPPY_QUORUM = new DeleteOption<Boolean>("SLOPPY_QUORUM");
    public static final DeleteOption<Integer> N_VAL = new DeleteOption<Integer>("N_VAL");

    private DeleteOption(String name)
    {
        super(name);
    }
}

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
package com.basho.riak.client.operations;

import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.cap.VClock;

public class FetchOption<T> extends RiakOption<T>
{

    static enum Type
    {
        R, PR, BASIC_QUORUM, NOTFOUND_OK, IF_MODIFIED, HEAD, DELETED_VCLOCK, TIMEOUT,
        SLOPPY_QUORUM, N_VAL;
    }
    
    public static final FetchOption<Quorum> R = new FetchOption<Quorum>(Type.R);
    public static final FetchOption<Quorum> PR = new FetchOption<Quorum>(Type.PR);
    public static final FetchOption<Boolean> BASIC_QUORUM = new FetchOption<Boolean>(Type.BASIC_QUORUM);
    public static final FetchOption<Boolean> NOTFOUND_OK = new FetchOption<Boolean>(Type.NOTFOUND_OK);
    public static final FetchOption<VClock> IF_MODIFIED = new FetchOption<VClock>(Type.IF_MODIFIED);
    public static final FetchOption<Boolean> HEAD = new FetchOption<Boolean>(Type.HEAD);
    public static final FetchOption<Boolean> DELETED_VCLOCK = new FetchOption<Boolean>(Type.DELETED_VCLOCK);
    public static final FetchOption<Integer> TIMEOUT = new FetchOption<Integer>(Type.TIMEOUT);
    public static final FetchOption<Boolean> SLOPPY_QUORUM = new FetchOption<Boolean>(Type.SLOPPY_QUORUM);
    public static final FetchOption<Integer> N_VAL = new FetchOption<Integer>(Type.N_VAL);

    private final Type type;
   
    private FetchOption(Type type)
    {
        super(type.name());
        this.type = type;
    }
    
    Type getType()
    {
        return type;
    }
    
}

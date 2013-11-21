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

public class StoreOption<T> extends RiakOption<T>
{

    public static final StoreOption<Quorum> W = new StoreOption<Quorum>("W");
    public static final StoreOption<Quorum> DW = new StoreOption<Quorum>("DW");
    public static final StoreOption<Quorum> PW = new StoreOption<Quorum>("PW");
    public static final StoreOption<Boolean> IF_NOT_MODIFIED = new StoreOption<Boolean>("IF_NOT_MODIFIED");
    public static final StoreOption<Boolean> IF_NONE_MATCH = new StoreOption<Boolean>("IF_NONE_MATCH");
    public static final StoreOption<Boolean> RETURN_HEAD = new StoreOption<Boolean>("RETURN_HEAD");
    public static final StoreOption<Integer> TIMEOUT = new StoreOption<Integer>("TIMEOUT");
    public static final StoreOption<Boolean> ASIS = new StoreOption<Boolean>("ASIS");
    public static final StoreOption<Boolean> SLOPPY_QUORUM = new StoreOption<Boolean>("SLOPPY_QUORUM");
    public static final StoreOption<Integer> N_VAL = new StoreOption<Integer>("N_VAL");
    public static final StoreOption<Boolean> RETURN_BODY = new StoreOption<Boolean>("RETURN_BODY");

    private StoreOption(String name)
    {
        super(name);
    }
}

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

 /*
 * @author Dave Rusek <drusuk at basho dot com>
 * @since 2.0
 */
public final class FetchOption<T> extends RiakOption<T>
{

    public static final FetchOption<Quorum> R = new FetchOption<Quorum>("R");
    public static final FetchOption<Quorum> PR = new FetchOption<Quorum>("PR");
    public static final FetchOption<Boolean> BASIC_QUORUM = new FetchOption<Boolean>("BASIC_QUORUM");
    public static final FetchOption<Boolean> NOTFOUND_OK = new FetchOption<Boolean>("NOTFOUND_OK");
    public static final FetchOption<VClock> IF_MODIFIED = new FetchOption<VClock>("IF_MODIFIED");
    public static final FetchOption<Boolean> HEAD = new FetchOption<Boolean>("HEAD");
    public static final FetchOption<Boolean> DELETED_VCLOCK = new FetchOption<Boolean>("DELETED_VCLOCK");
    public static final FetchOption<Integer> TIMEOUT = new FetchOption<Integer>("TIMEOUT");
    public static final FetchOption<Boolean> SLOPPY_QUORUM = new FetchOption<Boolean>("SLOPPY_QUORUM");
    public static final FetchOption<Integer> N_VAL = new FetchOption<Integer>("N_VAL");

    private FetchOption(String name)
    {
        super(name);
    }

}

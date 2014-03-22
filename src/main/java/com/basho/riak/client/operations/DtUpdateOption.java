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

 /*
 * @author Dave Rusek <drusuk at basho dot com>
 * @since 2.0
 */
public final class DtUpdateOption<T> extends RiakOption<T>
{

    public static final DtUpdateOption<Quorum> DW = new DtUpdateOption<Quorum>("DW");
    public static final DtUpdateOption<Integer> N_VAL = new DtUpdateOption<Integer>("N_VAL");
    public static final DtUpdateOption<Quorum> PW = new DtUpdateOption<Quorum>("PW");
    public static final DtUpdateOption<Boolean> RETURN_BODY = new DtUpdateOption<Boolean>("RETURN_BODY");
    public static final DtUpdateOption<Boolean> SLOPPY_QUORUM = new DtUpdateOption<Boolean>("SLOPPY_QUORUM");
    public static final DtUpdateOption<Integer> TIMEOUT = new DtUpdateOption<Integer>("TIMEOUT");
    public static final DtUpdateOption<Quorum> W = new DtUpdateOption<Quorum>("W");

    public DtUpdateOption(String name)
    {
        super(name);
    }
}

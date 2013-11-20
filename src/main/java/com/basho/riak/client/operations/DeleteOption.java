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

public class DeleteOption<T> extends RiakOption<T>
{

    public static final DeleteOption<Quorum> RW = new DeleteOption<Quorum>("RW");
    public static final DeleteOption<Quorum> R = new DeleteOption<Quorum>("R");
    public static final DeleteOption<Quorum> W = new DeleteOption<Quorum>("W");
    public static final DeleteOption<Quorum> PR = new DeleteOption<Quorum>("PR");
    public static final DeleteOption<Quorum> PW = new DeleteOption<Quorum>("PW");
    public static final DeleteOption<Quorum> DW = new DeleteOption<Quorum>("DW");
    public static final DeleteOption<Integer> TIMEOUT = new DeleteOption<Integer>("TIMEOUT");
    public static final DeleteOption<Boolean> SLOPPY_QUORUM = new DeleteOption<Boolean>("SLOPPY_QUORUM");
    public static final DeleteOption<Integer> N_VAL = new DeleteOption<Integer>("N_VAL");

    private DeleteOption(String name)
    {
        super(name);
    }
}

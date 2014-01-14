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

public class DtFetchOption<T> extends RiakOption<T> {

  public static final DtFetchOption<Quorum> R = new DtFetchOption<Quorum>("R");
  public static final DtFetchOption<Quorum> PR = new DtFetchOption<Quorum>("PR");
  public static final DtFetchOption<Boolean> BASIC_QUORUM = new DtFetchOption<Boolean>("BASIC_QUORUM");
  public static final DtFetchOption<Boolean> NOTFOUND_OK = new DtFetchOption<Boolean>("NOTFOUND_OK");
  public static final DtFetchOption<Integer> TIMEOUT = new DtFetchOption<Integer>("TIMEOUT");
  public static final DtFetchOption<Boolean> SLOPPY_QUORUM = new DtFetchOption<Boolean>("SLOPPY_QUORUM");
  public static final DtFetchOption<Integer> N_VAL = new DtFetchOption<Integer>("N_VAL");
  public static final DtFetchOption<Boolean> INCLUDE_CONTEXT = new DtFetchOption<Boolean>("INCLUDE_CONTEXT");

  public DtFetchOption(String name) {
    super(name);
  }
}

/**
 * This file is part of riak-java-pb-client 
 *
 * Copyright (c) 2010 by Trifork
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.basho.riak.pbc;

import java.io.IOException;

import com.basho.riak.protobuf.RiakPB.RpbErrorResp;

/**
 * Exception created from a PBC error response message
 */
@SuppressWarnings("serial")
public class RiakError extends IOException {

	public RiakError(RpbErrorResp err) {
		super(err.getErrmsg().toStringUtf8());
	}

}

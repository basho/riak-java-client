/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.basho.riak.client.http.plain;

import org.junit.Test;

import com.basho.riak.client.http.plain.ConvertToCheckedExceptions;
import com.basho.riak.client.http.plain.RiakIOException;
import com.basho.riak.client.http.plain.RiakResponseException;
import com.basho.riak.client.http.response.RiakIORuntimeException;
import com.basho.riak.client.http.response.RiakResponseRuntimeException;

public class TestConvertToCheckedExceptions {

    ConvertToCheckedExceptions impl = new ConvertToCheckedExceptions();
    
    @Test(expected=RiakIOException.class) public void translates_io_runtime_exception_to_checked() throws RiakIOException {
        impl.handle(new RiakIORuntimeException());
    }
    @Test(expected=RiakResponseException.class) public void translates_response_runtime_exception_to_checked() throws RiakResponseException {
        impl.handle(new RiakResponseRuntimeException(null));
    }
}
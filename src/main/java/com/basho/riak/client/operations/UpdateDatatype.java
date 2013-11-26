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

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.operations.datatypes.RiakDatatype;

import java.util.concurrent.ExecutionException;

public class UpdateDatatype<T extends RiakDatatype> extends RiakCommand<UpdateDatatype.Response<T>>
{
    @Override
    public Response<T> execute(RiakCluster cluster) throws ExecutionException, InterruptedException
    {
        return null;
    }

    public static class Response<T>
    {
        private final T datatype;

        Response(T datatype)
        {
            this.datatype = datatype;
        }

        public T getDatatype()
        {
            return datatype;
        }
    }
}

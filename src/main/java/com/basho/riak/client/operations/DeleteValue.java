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

import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.core.RiakCluster;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class DeleteValue implements RiakCommand<DeleteValue.Response>
{

    private final RiakCluster cluster;
    private final Key location;
    private final Map<DeleteOption<?>, Object> options;
    private VClock vClock;

    DeleteValue(RiakCluster cluster, Key location)
    {
        this.cluster = cluster;
        this.location = location;
        this.options = new HashMap<DeleteOption<?>, Object>();
    }

    @Override
    public Response execute() throws ExecutionException, InterruptedException
    {
        return null;
    }

    public DeleteValue withVClock(VClock vClock)
    {
        this.vClock = vClock;
        return this;
    }

    public <T> DeleteValue withOption(DeleteOption<T> option, T value)
    {
        options.put(option, value);
        return this;
    }

    public static class Response
    {
        private final boolean notFound;
        private final boolean deleted;

        public Response(boolean notFound, boolean deleted)
        {
            this.notFound = notFound;
            this.deleted = deleted;
        }

        public boolean isNotFound()
        {
            return notFound;
        }

        public boolean isDeleted()
        {
            return deleted;
        }
    }

}

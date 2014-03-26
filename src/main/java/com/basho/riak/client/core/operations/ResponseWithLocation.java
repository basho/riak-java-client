/*
 * Copyright 2014 Basho Technologies Inc.
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

package com.basho.riak.client.core.operations;

import com.basho.riak.client.query.Location;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
abstract class ResponseWithLocation
{
    private final Location location;
        
    protected ResponseWithLocation(Init<?> builder)
    {
        this.location = builder.location;
    }

    public Location getLocation()
    {
        return location;
    }

    protected static abstract class Init<T extends Init<T>>
    {
        private Location location;

        protected abstract T self();
        abstract ResponseWithLocation build();

        T withLocation(Location location)
        {
            this.location = location;
            return self();
        }
    }
}

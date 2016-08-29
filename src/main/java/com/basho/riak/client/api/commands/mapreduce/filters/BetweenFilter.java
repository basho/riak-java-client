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
package com.basho.riak.client.api.commands.mapreduce.filters;

/**
 * Key filter that matches keys in a range delimited by <code>from</code> -
 * <code>to</code>
 *
 * @author russell
 *
 */
public class BetweenFilter<T> extends KeyFilter
{

    private static final String NAME = "between";

    private final T from;
    private final T to;

    public BetweenFilter(T from, T to)
    {
        super(NAME);
        this.from = from;
        this.to = to;
    }

    public T getFrom()
    {
        return from;
    }

    public T getTo()
    {
        return to;
    }

}

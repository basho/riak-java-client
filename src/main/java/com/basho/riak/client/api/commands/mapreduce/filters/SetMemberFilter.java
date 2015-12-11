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

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Filter in keys that are a member of the provided set
 *
 * @author russell
 */
@JsonSerialize(using = SetMemberSerializer.class)
public class SetMemberFilter<T> extends KeyFilter
{

    private static final String NAME = "set_member";
    private final Set<T> set = new HashSet<T>();

    /**
     * Creates a set from a String var arg
     *
     * @param set
     */
    public SetMemberFilter(T... set)
    {
        super(NAME);
        Collections.addAll(this.set, set);
    }

    /**
     * Creates a set by copying a known set.
     * @param set
     */
    public SetMemberFilter(Set<T> set)
    {
        super(NAME);
        this.set.addAll(set);
    }

    public Set<T> getSet()
    {
        return set;
    }
}

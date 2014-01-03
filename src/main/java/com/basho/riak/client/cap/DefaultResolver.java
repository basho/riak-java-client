/*
 * Copyright 2013 Basho Technologies Inc.
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
package com.basho.riak.client.cap;

import java.util.List;

/**
 * A conflict resolver that doesn't resolve conflict. If it is presented with a
 * collection of siblings with more than one entry it throws an Exception
 *
 * @author Russell Brown <Russelldb at basho dot com>
 * @since 1.0
 */
public class DefaultResolver<T> implements ConflictResolver<T>
{

    /**
     * Detects conflict but does not resolve it.
     *
     * @return null or the single value in the collection
     * @throws UnresolvedConflictException if {@code siblings} has > 1 entry.
     */
    @Override
    public T resolve(List<T> siblings) throws UnresolvedConflictException
    {
        if (siblings.size() > 1)
        {
            throw new UnresolvedConflictException("Siblings found", siblings);
        }
        else if (siblings.size() == 1)
        {
            return siblings.get(0);
        }
        else
        {
            return null;
        }
    }
}

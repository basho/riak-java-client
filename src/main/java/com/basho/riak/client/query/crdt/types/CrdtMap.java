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
package com.basho.riak.client.query.crdt.types;

import com.basho.riak.client.util.ByteArrayWrapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

public class CrdtMap extends CrdtElement
{

    private final Map<ByteArrayWrapper, CrdtElement> entries =
        new HashMap<ByteArrayWrapper, CrdtElement>();

    public CrdtMap(List<MapEntry> entries)
    {
        for (MapEntry entry : entries)
        {
            this.entries.put(entry.field, entry.element);
        }
    }

    public CrdtElement get(ByteArrayWrapper key)
    {
        return entries.get(key);
    }

    /**
     * Get this CrdtMap as a {@link Map}. The returned asMap  is unmodifiable.
     *
     * @return a read-only view of the asMap
     */
    public Map<ByteArrayWrapper, CrdtElement> viewAsMap()
    {
        return unmodifiableMap(entries);
    }

    public static final class MapEntry
    {

        private final ByteArrayWrapper field;
        private final CrdtElement element;

        public MapEntry(ByteArrayWrapper field, CrdtElement element)
        {
            this.field = field;
            this.element = element;
        }

        public ByteArrayWrapper getField()
        {
            return field;
        }

        public CrdtElement getElement()
        {
            return element;
        }

    }


}

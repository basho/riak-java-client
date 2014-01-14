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
package com.basho.riak.client.convert;

import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.util.BinaryValue;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Converters
{

    public static <T> List<T> convert(Converter<T> converter, List<? extends RiakObject> objects)
    {
        if (objects == null)
        {
            return Collections.emptyList();
        }

        List<T> converted = new ArrayList<T>(objects.size());
        for (RiakObject o : objects)
        {
            converted.add(converter.toDomain(o));
        }
        return converted;
    }

    public static Converter<String> stringConverter()
    {
        return new Converter<String>()
        {
            @Override
            public String toDomain(RiakObject riakObject)
            {
                return new String(riakObject.getValue().unsafeGetValue());
            }

            @Override
            public RiakObject fromDomain(String domainObject)
            {
                RiakObject ro = new RiakObject();
                ro.setValue(BinaryValue.create(domainObject));
                return ro;
            }
        };
    }


    public static <T> Converter<T> jsonConverter()
    {
        return null;
    }
}

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
package com.basho.riak.client.api.convert;

import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;


/**
 * For working with {@link RiakObject} rather than domain types.
 * 
 * @author Russel Brown <russelldb at basho dot com>
 * @since 1.0
 */
public final class PassThroughConverter extends Converter<RiakObject>
{
    public PassThroughConverter()
    {
        super(RiakObject.class);
    }
    
    @Override
    public RiakObject toDomain(RiakObject obj, Location location)
    {
        return obj;
    }
    
    @Override
    public Converter.OrmExtracted fromDomain(RiakObject domainObject, Namespace namespace, BinaryValue key)
    {
        return new Converter.OrmExtracted(domainObject, namespace, key);
    }
    
    

    @Override
    public RiakObject toDomain(BinaryValue value, String contentType)
    {
        throw new UnsupportedOperationException("Not supported"); 
    }

    @Override
    public ContentAndType fromDomain(RiakObject domainObject)
    {
        throw new UnsupportedOperationException("Not supported"); 
    }
    
}

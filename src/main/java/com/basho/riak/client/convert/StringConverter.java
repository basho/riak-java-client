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

package com.basho.riak.client.convert;

import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.util.BinaryValue;

/**
 * Converter that passes Strings through unmodified.
 * 
 * Worth noting is that when using String directly with StoreValue
 * there's no way to determine the character set. This converter uses the
 * default character set for conversion. 
 * 
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class StringConverter extends Converter<String>
{

    public StringConverter()
    {
        super(String.class);
    }
    
    @Override
    public String toDomain(RiakObject obj, Location location)
    {
        return obj.getValue().toString();
    }
    
    @Override
    public Converter.OrmExtracted fromDomain(String domainObject, Location location)
    {
        RiakObject obj = new RiakObject()
                        .setValue(BinaryValue.create(domainObject))
                        .setContentType("text/plain");
        return new Converter.OrmExtracted(obj, location);
    }
    
    @Override
    public String toDomain(BinaryValue value, String contentType) throws ConversionException
    {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public ContentAndType fromDomain(String domainObject) throws ConversionException
    {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    
    
}

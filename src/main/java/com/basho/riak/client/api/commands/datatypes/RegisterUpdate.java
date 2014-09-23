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
package com.basho.riak.client.api.commands.datatypes;

import com.basho.riak.client.core.query.crdt.ops.RegisterOp;
import com.basho.riak.client.core.util.BinaryValue;

/**
 * An update to a Riak register datatype.
 * <p>
 * When building an {@link UpdateMap} command
 * this class is used to encapsulate the update to be performed on a 
 * Riak register datatype contained in the map. It is used in conjunction with the
 * {@link MapUpdate}.
 * </p>
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public class RegisterUpdate implements DatatypeUpdate
{

    private final BinaryValue value;

    /**
     * Construct a RegisterUpdate with the provided bytes. 
     * @param value the bytes representing the register.
     */
    public RegisterUpdate(byte[] value)
    {
        this.value = BinaryValue.create(value);
    }

    /**
     * Construct a RegisterUpdate with the provided BinaryValue. 
     * @param value the BinaryValue representing the register.
     */
    public RegisterUpdate(BinaryValue value)
    {
        this.value = value;
    }
    
    /**
     * Construct a RegisterUpdate with the provided String. 
     * <p>
     * Note the String is converted to bytes using the default Charset.
     * </p>
     * @param value the String representing the register.
     */
    public RegisterUpdate(String value)
    {
        this.value = BinaryValue.create(value);
    }
    
    /**
     * Get the register contained in this update.
     * @return the register as a BinaryValue.
     */
    public BinaryValue get()
    {
        return value;
    }

    /**
     * Returns the core update.
     * @return the update used by the client core.
     */
    @Override
    public RegisterOp getOp()
    {
        return new RegisterOp(value);
    }

    @Override
    public String toString()
    {
        return value.toString();
    }

}

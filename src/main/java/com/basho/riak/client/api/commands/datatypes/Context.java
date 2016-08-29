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

import com.basho.riak.client.core.util.BinaryValue;

 /**
  * Encapsulates a context returned from a datatype fetch command.
  * <p>
  * When performing an update to a datatype, the Context from the
  * previous fetch is used.
  * </p>
  * @author Dave Rusek <drusek at basho dot com>
  * @since 2.0
  */
public class Context
{

    private final BinaryValue value;

    /**
     * Construct a new Context.
     * @param value
     */
    public Context(BinaryValue value)
    {
        this.value = value;
    }

    /**
     * Returns the context as a BinaryValue.
     * @return the context.
     */
    public BinaryValue getValue()
    {
        return value;
    }
}

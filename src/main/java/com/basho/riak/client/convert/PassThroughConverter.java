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
package com.basho.riak.client.convert;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.cap.VClock;

/**
 * For working with {@link IRiakObject} rather than domain types.
 * @author russell
 *
 */
public class PassThroughConverter implements Converter<IRiakObject> {

    /* (non-Javadoc)
     * @see com.basho.riak.client.convert.Converter#fromDomain(java.lang.Object, com.basho.riak.client.cap.VClock)
     */
    public IRiakObject fromDomain(IRiakObject domainObject, VClock vclock) throws ConversionException {
        return domainObject;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.convert.Converter#toDomain(com.basho.riak.client.IRiakObject)
     */
    public IRiakObject toDomain(IRiakObject riakObject) throws ConversionException {
        return riakObject;
    }
}

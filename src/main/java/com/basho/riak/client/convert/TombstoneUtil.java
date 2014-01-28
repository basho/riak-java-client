/*
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

import com.basho.riak.client.convert.reflect.AnnotationHelper;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class TombstoneUtil
{
    /**
     * Attempts to inject <code>isTombstone</code> as the value of the {@link RiakTombstone}
     * annotated field of <code>domainObject</code>
     * 
     * @param <T>
     *            the type of <code>domainObject</code>
     * @param domainObject
     *            the object to inject the key into
     * @param isTombstone
     *            the boolean to inject
     * @return <code>domainObject</code> with {@link RiakTombstone} annotated field
     *         set to <code>isTombstone</code>
     * @throws ConversionException
     *             if there is a {@link RiakTombstone} annotated field but it cannot
     *             be set to the value of <code>isTombstone</code>
     */
    public static <T> T setTombstone(T domainObject, boolean isTombstone) throws ConversionException {
        T obj = AnnotationHelper.getInstance().setRiakTombstone(domainObject, isTombstone);
        return obj;
    }
    
    /**
     * Attempts to get boolean from <code>domainObject</code> by looking for a
     * {@link RiakTombstone} annotated field. If non-present it simply returns
     * <code>null</code>
     * 
     * @param <T>
     *            the type of <code>domainObject</code>
     * @param domainObject
     *            the object to search for a key
     * @return either the value found on <code>domainObject</code>'s
     *         {@link RiakTombstone} field or <code>null</code>
     */
    public static <T> Boolean getTombstone(T domainObject) {
        return AnnotationHelper.getInstance().getRiakTombstone(domainObject);
    }
}

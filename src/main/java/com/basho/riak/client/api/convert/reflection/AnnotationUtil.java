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
package com.basho.riak.client.api.convert.reflection;

import com.basho.riak.client.api.cap.VClock;
import com.basho.riak.client.api.convert.ConversionException;
import com.basho.riak.client.core.query.UserMetadata.RiakUserMetadata;
import com.basho.riak.client.core.query.indexes.RiakIndexes;
import com.basho.riak.client.core.query.links.RiakLinks;
import com.basho.riak.client.core.util.BinaryValue;

/**
 * Static utility methods used to get/set annotated fields/methods.
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class AnnotationUtil
{
    private AnnotationUtil() {}
    
    /**
     * Attempts to inject <code>key</code> as the value of the {@literal @RiakKey}
     * annotated member of <code>domainObject</code>
     *
     * @param <T> the type of <code>domainObject</code>
     * @param domainObject the object to inject the key into
     * @param key the key to inject
     * @return <code>domainObject</code> with {@literal @RiakKey} annotated member
     * set to <code>key</code>
     * @throws ConversionException if there is a {@literal @RiakKey} annotated member
     * but it cannot be set to the value of <code>key</code>
     */
    public static <T> T setKey(T domainObject, BinaryValue key) throws ConversionException
    {
        T obj = AnnotationHelper.getInstance().setRiakKey(domainObject, key);
        return obj;
    }

    /**
     * Attempts to get a key from <code>domainObject</code> by looking for a
     * {@literal @RiakKey} annotated member. If non-present it simply returns
     * <code>defaultKey</code>
     *
     * @param <T> the type of <code>domainObject</code>
     * @param domainObject the object to search for a key
     * @param defaultKey the pass through value that will get returned if no key
     * found on <code>domainObject</code>
     * @return either the value found on <code>domainObject</code>;s
     * {@literal @RiakKey} member or <code>defaultkey</code>
     */
    public static <T> BinaryValue getKey(T domainObject, BinaryValue defaultKey)
    {
        BinaryValue key = getKey(domainObject);
        if (key == null)
        {
            key = defaultKey;
        }
        return key;
    }

    /**
     * Attempts to get a key from <code>domainObject</code> by looking for a
     * {@literal @RiakKey} annotated member. If non-present it simply returns
     * <code>null</code>
     *
     * @param <T> the type of <code>domainObject</code>
     * @param domainObject the object to search for a key
     * @return either the value found on <code>domainObject</code>;s
     * {@literal @RiakKey} member or <code>null</code>
     */
    public static <T> BinaryValue getKey(T domainObject)
    {
        return AnnotationHelper.getInstance().getRiakKey(domainObject);
    }

    public static <T> T setBucketName(T domainObject, BinaryValue bucketName)
    {
        return AnnotationHelper.getInstance().setRiakBucketName(domainObject, bucketName);
    }
    
    public static <T> BinaryValue getBucketName(T domainObject, BinaryValue defaultBucketName)
    {
        BinaryValue bucketName = getBucketName(domainObject);
        return bucketName != null ? bucketName : defaultBucketName;
    }
    
    public static <T> BinaryValue getBucketName(T domainObject)
    {
        return AnnotationHelper.getInstance().getRiakBucketName(domainObject);
    }
    
    public static <T> T setBucketType(T domainObject, BinaryValue bucketType)
    {
        return AnnotationHelper.getInstance().setRiakBucketType(domainObject, bucketType);
    }
    
    public static <T> BinaryValue getBucketType(T domainObject, BinaryValue defaultBucketType)
    {
        BinaryValue bucketType = getBucketType(domainObject);
        return bucketType != null ? bucketType : defaultBucketType;
    }
    
    public static <T> BinaryValue getBucketType(T domainObject)
    {
        return AnnotationHelper.getInstance().getRiakBucketType(domainObject);
    }
    
    public static <T> boolean hasVClockAnnotation(T domainObject)
    {
        return AnnotationHelper.getInstance().hasRiakVClockAnnotation(domainObject);
    }
    
    /**
     * Attempts to inject <code>vclock</code> as the value of the
     * {@literal @RiakVClock} annotated member of <code>domainObject</code>
     *
     * @param <T> the type of <code>domainObject</code>
     * @param domainObject the object to inject the key into
     * @param vclock the vclock to inject
     * @return <code>domainObject</code> with {@literal @RiakVClock} annotated member
     * set to <code>vclock</code>
     * @throws ConversionException if there is a {@literal @RiakVClock} annotated
     * member but it cannot be set to the value of <code>vclock</code>
     */
    public static <T> T setVClock(T domainObject, VClock vclock) throws ConversionException
    {
        T obj = AnnotationHelper.getInstance().setRiakVClock(domainObject, vclock);
        return obj;
    }

    public static <T> VClock getVClock(T domainObject, VClock defaultVClock)
    {
        VClock vclock = getVClock(domainObject);
        return vclock != null ? vclock : defaultVClock;
    }
    
    /**
     * Attempts to get a vector clock from <code>domainObject</code> by looking
     * for a {@literal @RiakVClock} annotated member. If non-present it simply
     * returns <code>null</code>
     *
     * @param <T> the type of <code>domainObject</code>
     * @param domainObject the object to search for a key
     * @return either the value found on <code>domainObject</code>;s
     * {@literal @RiakVClock} member or <code>null</code>
     */
    public static <T> VClock getVClock(T domainObject)
    {
        return AnnotationHelper.getInstance().getRiakVClock(domainObject);
    }

    /**
     * Attempts to inject <code>isTombstone</code> as the value of the
     * {@literal @RiakTombstone} annotated member of <code>domainObject</code>
     *
     * @param <T> the type of <code>domainObject</code>
     * @param domainObject the object to inject the key into
     * @param isTombstone the boolean to inject
     * @return <code>domainObject</code> with {@literal @RiakTombstone} annotated
     * member set to <code>isTombstone</code>
     * @throws ConversionException if there is a {@literal @RiakTombstone} annotated
     * member but it cannot be set to the value of <code>isTombstone</code>
     */
    public static <T> T setTombstone(T domainObject, boolean isTombstone) throws ConversionException
    {
        T obj = AnnotationHelper.getInstance().setRiakTombstone(domainObject, isTombstone);
        return obj;
    }

    /**
     * Attempts to get boolean from <code>domainObject</code> by looking for a
     * {@literal @RiakTombstone} annotated member. If non-present it simply returns
     * <code>null</code>
     *
     * @param <T> the type of <code>domainObject</code>
     * @param domainObject the object to search for a key
     * @return either the value found on <code>domainObject</code>'s
     * {@literal @RiakTombstone} member or <code>null</code>
     */
    public static <T> Boolean getTombstone(T domainObject)
    {
        return AnnotationHelper.getInstance().getRiakTombstone(domainObject);
    }

    /**
     * Attempts to get all the riak indexes from a domain object by looking for
     * a {@literal @RiakIndexes} annotated member.
     * <p>
     * If no indexes are present, an empty RiakIndexes is returned.</p>
     *
     * @param <T> the type of the domain object
     * @param domainObject the domain object
     * @return a RiakIndexes
     */
    public static <T> RiakIndexes getIndexes(RiakIndexes container, T domainObject)
    {
        return AnnotationHelper.getInstance().getIndexes(container, domainObject);
    }

    /**
     * Attempts to populate a domain object with the contents of the supplied
     * RiakIndexes by looking for a {@literal @RiakIndex} annotated member
     *
     * @param <T> the type of the domain object
     * @param indexes a populated RiakIndexes object.
     * @param domainObject the domain object
     * @return the domain object.
     */
    public static <T> T populateIndexes(RiakIndexes indexes, T domainObject)
    {
        return AnnotationHelper.getInstance().setIndexes(indexes, domainObject);
    }

    /**
     * Attempts to get the the Riak links from a domain object by looking for a
     * {@literal @RiakLinks} annotated member.
     *
     * @param <T> the domain object type
     * @param container the RiakLinks container
     * @param domainObject the domain object
     * @return a Collection of RiakLink objects.
     */
    public static <T> RiakLinks getLinks(RiakLinks container, T domainObject)
    {
        return AnnotationHelper.getInstance().getLinks(container, domainObject);
    }

    /**
     * Attempts to populate a domain object with riak links by looking for a
     * {@literal @RiakLinks} annotated member.
     *
     * @param <T> the type of the domain object
     * @param links a collection of RiakLink objects
     * @param domainObject the domain object
     * @return the domain object
     */
    public static <T> T populateLinks(RiakLinks links, T domainObject)
    {
        return AnnotationHelper.getInstance().setLinks(links, domainObject);
    }

    /**
     * Attempts to get the riak user metadata from a domain object by looking
     * for a {@literal @RiakUsermeta} annotated field or getter method.
     *
     * @param <T> the type of the domain object
     * @param metaContainer the RiakUserMetadata container
     * @param domainObject the domain object
     * @return a Map containing the user metadata.
     */
    public static <T> RiakUserMetadata getUsermetaData(RiakUserMetadata metaContainer, T domainObject)
    {
        return AnnotationHelper.getInstance().getUsermetaData(metaContainer, domainObject);
    }

    /**
     * Attempts to populate a domain object with user metadata by looking for a
     * {@literal @RiakUsermeta} annotated member.
     *
     * @param <T> the type of the domain object
     * @param usermetaData a Map of user metadata.
     * @param domainObject the domain object.
     * @return the domain object.
     */
    public static <T> T populateUsermeta(RiakUserMetadata usermetaData, T domainObject)
    {
        return AnnotationHelper.getInstance().setUsermetaData(usermetaData, domainObject);
    }
    
    public static <T> String getContentType(T domainObject, String defaultContentType)
    {
        String type = getContentType(domainObject);
        return type != null ? type : defaultContentType;
    }
    
    public static <T> String getContentType(T domainObject)
    {
        return AnnotationHelper.getInstance().getRiakContentType(domainObject);
    }
    
    public static <T> T setContentType(T domainObject, String contentType)
    {
        return AnnotationHelper.getInstance().setRiakContentType(domainObject, contentType);
    }
    
    public static <T> T setVTag(T domainObject, String vtag)
    {
        return AnnotationHelper.getInstance().setRiakVTag(domainObject, vtag);
    }
    
    public static <T> T setLastModified(T domainObject, Long lastModified)
    {
        return AnnotationHelper.getInstance().setRiakLastModified(domainObject, lastModified);
    }
    
}

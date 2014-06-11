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

import com.basho.riak.client.convert.reflection.AnnotationUtil;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.Namespace;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.util.BinaryValue;
import java.lang.reflect.Constructor;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * The Converter acts as a bridge between the core and the user level API, specifically 
 * for ORM.
 * <p>
 * Subclasses will override the {@link #fromDomain(java.lang.Object) } and 
 * {@link #fromDomain(java.lang.Object) } methods to convert the value portion 
 * of a {@code RiakObject} to a domain object. 
 * </p>
 * 
 * @author Brian Roach <roach at basho dot com>
 * @param <T> the type to convert to
 * @since 2.0
 */
public abstract class Converter<T>
{
    protected final Type type;
    
    public Converter(Type type)
    {
        this.type = type;
    }
    
    @SuppressWarnings("unchecked")
    protected final T newDomainInstance()
    {
        try
        {
            Class<?> rawType = type instanceof Class<?>
                ? (Class<?>) type
                : (Class<?>) ((ParameterizedType) type).getRawType();
            Constructor<?> constructor = rawType.getConstructor();
            return (T) constructor.newInstance();
        }
        catch (Exception ex)
        {
            throw new ConversionException(ex);
        }
    }
    
    /**
     * Converts from a RiakObject to a domain object.
     * 
     * @param obj the RiakObject to be converted
     * @param location The location of this RiakObject in Riak
     * @return an instance of the domain type T
     */
    public T toDomain(RiakObject obj, Location location)
    {
        T domainObject;
        if (obj.isDeleted())
        {
            domainObject = newDomainInstance();
        }
        else
        {
            domainObject = toDomain(obj.getValue(), obj.getContentType());

            AnnotationUtil.populateIndexes(obj.getIndexes(), domainObject);
            AnnotationUtil.populateLinks(obj.getLinks(), domainObject);
            AnnotationUtil.populateUsermeta(obj.getUserMeta(), domainObject);
            AnnotationUtil.setContentType(domainObject, obj.getContentType());
            AnnotationUtil.setVTag(domainObject, obj.getVTag());
        }

        AnnotationUtil.setKey(domainObject, location.getKey());
        AnnotationUtil.setBucketName(domainObject, location.getNamespace().getBucketName());
        AnnotationUtil.setBucketType(domainObject, location.getNamespace().getBucketType());

        AnnotationUtil.setVClock(domainObject, obj.getVClock());
        AnnotationUtil.setTombstone(domainObject, obj.isDeleted());
        AnnotationUtil.setLastModified(domainObject, obj.getLastModified());

        return domainObject;
    
    }
    
    /**
     * Convert the value portion of a RiakObject to a domain object.
     * <p>
     * Implementations override this method to convert the value contained in
     * a {@code RiakObject} to an instance of a domain object. 
     * </p>
     * @param value the value portion of a RiakObject to convert to a domain object
     * @param contentType The content type of the RiakObject
     * @return a new instance of the domain object
     */
    public abstract T toDomain(BinaryValue value, String contentType) throws ConversionException;
    
    /**
     * Convert from a domain object to a RiakObject.
     * <p>
     * The domain object itself may be completely annotated with everything 
     * required to produce a RiakObject except for the value portion. 
     * This will prefer annotated
     * items over the {@code Location} passed in.
     * </p>
     * @param domainObject a domain object to be stored in Riak.
     * @param namespace the namespace in Riak
     * @param key the key for the object
     * @return data to be stored in Riak.
     */
    public OrmExtracted fromDomain(T domainObject, Namespace namespace, BinaryValue key)
    {        
        BinaryValue bucketName = namespace != null ? namespace.getBucketName() : null;
        BinaryValue bucketType = namespace != null ? namespace.getBucketType() : null;
        
        key = AnnotationUtil.getKey(domainObject, key);
        bucketName = AnnotationUtil.getBucketName(domainObject, bucketName);
        bucketType = AnnotationUtil.getBucketType(domainObject, bucketType);
        
        if (bucketName == null)
        {
            throw new ConversionException("Bucket name not provided via namespace or domain object");
        }
        
        VClock vclock = AnnotationUtil.getVClock(domainObject);
        String contentType = 
            AnnotationUtil.getContentType(domainObject, RiakObject.DEFAULT_CONTENT_TYPE);
        
        RiakObject riakObject = new RiakObject();
        
        AnnotationUtil.getUsermetaData(riakObject.getUserMeta(), domainObject);
        AnnotationUtil.getIndexes(riakObject.getIndexes(), domainObject);
        AnnotationUtil.getLinks(riakObject.getLinks(), domainObject);
        
        ContentAndType cAndT = fromDomain(domainObject);
        contentType = cAndT.contentType != null ? cAndT.contentType : contentType;
        
        riakObject.setContentType(contentType)
                    .setValue(cAndT.content)
                    .setVClock(vclock);
        
        // We allow an annotated object to omit @BucketType and get the default
        Namespace ns;
        if (bucketType == null)
        {
            ns = new Namespace(bucketName);
        }
        else
        {
            ns = new Namespace(bucketType, bucketName);
        }
        
        OrmExtracted extracted = new OrmExtracted(riakObject, ns, key);
        return extracted;
    }
    
    /**
     * Provide the value portion of a RiakObject from the domain object.
     * <p>
     * Implementations override this method to provide the value portion of the 
     * RiakObject to be stored from the supplied domain object.
     * </p>
     * @param domainObject the domain object.
     * @return A BinaryValue to be stored in Riak
     */
    public abstract ContentAndType fromDomain(T domainObject) throws ConversionException;
    
    /**
     * Encapsulation of ORM data extracted from a domain object.
     * <p>
     * This allows for user-defined POJOs to encapsulate everything required to 
     * query Riak.
     *</p>
     * 
     */
    public static class OrmExtracted
    {
        private final RiakObject riakObject;
        private final Namespace namespace;
        private final BinaryValue key;
        
        public OrmExtracted(RiakObject riakObject, Namespace namespace, BinaryValue key)
        {
            this.riakObject = riakObject;
            this.namespace = namespace;
            this.key = key;
        }

        /**
         * @return the riakObject
         */
        public RiakObject getRiakObject()
        {
            return riakObject;
        }

        /**
         * @return the namespace
         */
        public Namespace getNamespace()
        {
            return namespace;
        }
        
        public boolean hasKey()
        {
            return key != null;
        }
        
        public BinaryValue getKey()
        {
            return key;
        }
        
    }

    protected class ContentAndType
    {
        private final BinaryValue content;
        private final String contentType;
        
        public ContentAndType(BinaryValue content, String contentType)
        {
            this.content = content;
            this.contentType = contentType;
        }
    }
}

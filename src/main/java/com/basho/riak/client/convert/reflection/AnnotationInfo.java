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
package com.basho.riak.client.convert.reflection;

import static com.basho.riak.client.convert.reflection.ClassUtil.getFieldValue;
import static com.basho.riak.client.convert.reflection.ClassUtil.setFieldValue;
import static com.basho.riak.client.convert.reflection.ClassUtil.getMethodValue;
import static com.basho.riak.client.convert.reflection.ClassUtil.setMethodValue;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.basho.riak.client.query.links.RiakLink;
import com.basho.riak.client.cap.BasicVClock;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.query.UserMetadata.RiakUserMetadata;
import com.basho.riak.client.query.indexes.LongIntIndex;
import com.basho.riak.client.query.indexes.RiakIndexes;
import com.basho.riak.client.query.indexes.StringBinIndex;
import com.basho.riak.client.query.links.RiakLinks;
import com.basho.riak.client.util.BinaryValue;
import java.lang.reflect.Method;
import java.util.LinkedList;

/**
 * Class that containsKeyKey the Riak annotated fields for an annotated class
 *
 * @author russell
 *
 */
public class AnnotationInfo
{

    private final Field riakKeyField;
    private final Method riakKeySetter;
    private final Method riakKeyGetter;
    private final List<UsermetaField> usermetaFields;
    private final List<UsermetaMethod> usermetaMethods;
    private final List<RiakIndexField> indexFields;
    private final List<RiakIndexMethod> indexMethods;
    private final Field riakLinksField;
    private final Method riakLinksGetter;
    private final Method riakLinksSetter;
    private final Field riakVClockField;
    private final Method riakVClockSetter;
    private final Method riakVClockGetter;
    private final Field riakTombstoneField;
    private final Method riakTombstoneSetter;
    private final Method riakTombstoneGetter;
    private final Field riakContentTypeField;
    private final Method riakContentTypeGetter;
    private final Method riakContentTypeSetter;
    private final Field riakLastModifiedField;
    private final Method riakLastModifiedSetter;
    private final Field riakVTagField;
    private final Method riakVTagSetter;
    private final Field riakBucketNameField;
    private final Method riakBucketNameSetter;
    private final Method riakBucketNameGetter;
    private final Field riakBucketTypeField;
    private final Method riakBucketTypeSetter;
    private final Method riakBucketTypeGetter;

    private AnnotationInfo(Builder builder)
    {
        this.riakKeyField = builder.riakKeyField;
        this.riakKeyGetter = builder.riakKeyGetter;
        this.riakKeySetter = builder.riakKeySetter;
        this.riakLinksField = builder.riakLinksField;
        this.riakLinksGetter = builder.riakLinksGetter;
        this.riakLinksSetter = builder.riakLinksSetter;
        this.riakVClockField = builder.riakVClockField;
        this.riakVClockGetter = builder.riakVClockGetter;
        this.riakVClockSetter = builder.riakVClockSetter;
        this.riakTombstoneField = builder.riakTombstoneField;
        this.riakTombstoneGetter = builder.riakTombstoneGetter;
        this.riakTombstoneSetter = builder.riakTombstoneSetter;
        this.usermetaFields = builder.usermetaFields;
        this.usermetaMethods = builder.usermetaMethods;
        this.indexFields = builder.indexFields;
        this.indexMethods = builder.indexMethods;
        this.riakContentTypeField = builder.riakContentTypeField;
        this.riakContentTypeGetter = builder.riakContentTypeGetter;
        this.riakContentTypeSetter = builder.riakContentTypeSetter;
        this.riakLastModifiedField = builder.riakLastModifiedField;
        this.riakLastModifiedSetter = builder.riakLastModified;
        this.riakVTagField = builder.riakVTagField;
        this.riakVTagSetter = builder.riakVTagSetter;
        this.riakBucketNameField = builder.riakBucketNameField;
        this.riakBucketNameGetter = builder.riakBucketNameGetter;
        this.riakBucketNameSetter = builder.riakBucketNameSetter;
        this.riakBucketTypeField = builder.riakBucketTypeField;
        this.riakBucketTypeGetter = builder.riakBucketTypeGetter;
        this.riakBucketTypeSetter = builder.riakBucketTypeSetter;
    }

    /**
     * Returns the key.
     * <p>
     * The @RiakKey annotation allows for any type to be used. this method will
     * call the object's toString() method to return a String.
     * </p>
     *
     * @param <T> the type of the domain object
     * @param obj the domain object
     * @return the String representation of the key
     */
    public <T> BinaryValue getRiakKey(T obj)
    {
        // TODO: charset
        BinaryValue key = null;
        if (riakKeyField != null)
        {
            if (riakKeyField.getType().equals(String.class))
            {
                Object o = getFieldValue(riakKeyField, obj);
                if (o != null)
                {
                    key = BinaryValue.create((String) o);
                }
            }
            else
            {
                Object o = getFieldValue(riakKeyField, obj);
                if (o != null)
                {
                    key = BinaryValue.create((byte[]) o);
                }
            }
        }
        else if (riakKeyGetter != null)
        {
            if (riakKeyGetter.getReturnType().isArray())
            {
                Object o = getMethodValue(riakKeyGetter, obj);
                if (o != null)
                {
                    key = BinaryValue.create((byte[]) o);
                }
            }
            else
            {
                Object o = getMethodValue(riakKeyGetter, obj);
                if (o != null)
                {
                    key = BinaryValue.create((String) o);
                }
            }
        }
        return key;
    }

    // TODO: charset
    public <T> void setRiakKey(T obj, BinaryValue key)
    {
        if (riakKeyField != null)
        {
            if (riakKeyField.getType().equals(String.class))
            {
                setFieldValue(riakKeyField, obj, key.toString());
            }
            else
            {
                setFieldValue(riakKeyField, obj, key.unsafeGetValue());
            }
        }
        else if (riakKeySetter != null)
        {
            if (riakKeySetter.getParameterTypes()[0].isArray())
            {
                setMethodValue(riakKeySetter, obj, key.unsafeGetValue());
            }
            else
            {
                setMethodValue(riakKeySetter, obj, key.toString());
            }
        }
    }

    // TODO: charset in annotation
    public <T> BinaryValue getRiakBucketName(T obj)
    {
        BinaryValue bucketName = null;
        
        if (riakBucketNameField != null)
        {
            if (riakBucketNameField.getType().equals(String.class))
            {
                Object o = getFieldValue(riakBucketNameField, obj);
                if (o != null)
                {
                    bucketName = BinaryValue.create((String) o);
                }
            }
            else
            {
                Object o = getFieldValue(riakBucketNameField, obj);
                if (o != null)
                {
                    bucketName = BinaryValue.create((byte[]) o);
                }
            }
        }
        else if (riakBucketNameGetter != null)
        {
            if (riakBucketNameGetter.getReturnType().isArray())
            {
                Object o = getMethodValue(riakBucketNameGetter, obj);
                if (o != null)
                {
                    bucketName = BinaryValue.create((byte[]) o);
                }
            }
            else
            {
                Object o = getMethodValue(riakBucketNameGetter, obj);
                if (o != null)
                {
                    bucketName = BinaryValue.create((String) o);
                }
            }
        }
        return bucketName;
    }
    
    // TODO: charset in annotation
    public <T> void setRiakBucketName(T obj, BinaryValue bucketName)
    {
        if (riakBucketNameField != null)
        {
            if (riakBucketNameField.getType().equals(String.class))
            {
                setFieldValue(riakBucketNameField, obj, bucketName.toString());
            }
            else
            {
                setFieldValue(riakBucketNameField, obj, bucketName.unsafeGetValue());
            }
        }
        else if (riakBucketNameSetter != null)
        {
            if (riakBucketNameSetter.getParameterTypes()[0].isArray())
            {
                setMethodValue(riakBucketNameSetter, obj, bucketName.unsafeGetValue());
            }
            else
            {
                setMethodValue(riakBucketNameSetter, obj, bucketName.toString());
            }
        }
    }
    
    // TODO: charset in annotation
    public <T> BinaryValue getRiakBucketType(T obj)
    {
        BinaryValue bucketType = null;
        
        if (riakBucketTypeField != null)
        {
            if (riakBucketTypeField.getType().equals(String.class))
            {
                Object o = getFieldValue(riakBucketTypeField, obj);
                if (o != null)
                {
                    bucketType = BinaryValue.create((String) o);
                }
            }
            else
            {
                Object o = getFieldValue(riakBucketTypeField, obj);
                if (o != null)
                {
                    bucketType = BinaryValue.create((byte[]) o);
                }
            }
        }
        else if (riakBucketTypeGetter != null)
        {
            if (riakBucketTypeGetter.getReturnType().isArray())
            {
                Object o = getMethodValue(riakBucketTypeGetter, obj);
                if (o != null)
                {
                    bucketType = BinaryValue.create((byte[]) o);
                }
            }
            else
            {
                Object o = getMethodValue(riakBucketTypeGetter, obj);
                if (o != null)
                {
                    bucketType = BinaryValue.create((String) o);
                }
            }
        }
        return bucketType;
    }
    
    // TODO: charset in annotation
    public <T> void setRiakBucketType(T obj, BinaryValue bucketType)
    {
        if (riakBucketTypeField != null)
        {
            if (riakBucketTypeField.getType().equals(String.class))
            {
                setFieldValue(riakBucketTypeField, obj, bucketType.toString());
            }
            else
            {
                setFieldValue(riakBucketTypeField, obj, bucketType.unsafeGetValue());
            }
        }
        else if (riakBucketTypeSetter != null)
        {
            if (riakBucketTypeSetter.getParameterTypes()[0].isArray())
            {
                setMethodValue(riakBucketTypeSetter, obj, bucketType.unsafeGetValue());
            }
            else
            {
                setMethodValue(riakBucketTypeSetter, obj, bucketType.toString());
            }
        }
    }
    
    public <T> VClock getRiakVClock(T obj)
    {

        VClock vclock = null;

        // We allow the annotated field to be either an actual VClock, or
        // a byte array.
        if (riakVClockField != null)
        {
            if (riakVClockField.getType().isAssignableFrom(VClock.class))
            {
                vclock = (VClock) getFieldValue(riakVClockField, obj);
            }
            else
            {
                vclock = new BasicVClock((byte[]) getFieldValue(riakVClockField, obj));
            }
        }
        else if (riakVClockGetter != null)
        {
            if (riakVClockGetter.getReturnType().isArray())
            {
                vclock = new BasicVClock((byte[]) getMethodValue(riakVClockGetter, obj));
            }
            else
            {
                vclock = (VClock) getMethodValue(riakVClockGetter, obj);
            }
        }

        return vclock;
    }

    public <T> void setRiakVClock(T obj, VClock vclock)
    {

        // We allow the annotated field to be either an actual VClock, or
        // a byte array. This is enforced in the AnnotationScanner
        if (riakVClockField != null)
        {

            if (riakVClockField.getType().isAssignableFrom(VClock.class))
            {
                setFieldValue(riakVClockField, obj, vclock);
            }
            else
            {
                setFieldValue(riakVClockField, obj, vclock.getBytes());
            }
        }
        else if (riakVClockSetter != null)
        {
            Class<?> pType = riakVClockSetter.getParameterTypes()[0];
            if (pType.isArray())
            {
                setMethodValue(riakVClockSetter, obj, vclock.getBytes());
            }
            else
            {
                setMethodValue(riakVClockSetter, obj, vclock);
            }
        }
    }

    public <T> Boolean getRiakTombstone(T obj)
    {
        Boolean tombstone = null;
        if (riakTombstoneField != null)
        {
            tombstone = (Boolean) getFieldValue(riakTombstoneField, obj);
        }
        else if (riakTombstoneGetter != null)
        {
            tombstone = (Boolean) getMethodValue(riakTombstoneGetter, obj);
        }

        return tombstone;
    }

    public <T> void setRiakTombstone(T obj, Boolean isDeleted)
    {

        if (riakTombstoneField != null)
        {
            setFieldValue(riakTombstoneField, obj, isDeleted);
        }
        else if (riakTombstoneSetter != null)
        {
            setMethodValue(riakTombstoneSetter, obj, isDeleted);
        }
    }

    public <T> String getRiakContentType(T obj)
    {
        String contentType = null;
        if (riakContentTypeField != null)
        {
            contentType = (String) getFieldValue(riakContentTypeField, obj);
        }
        else if (riakContentTypeGetter != null)
        {
            contentType = (String) getMethodValue(riakContentTypeGetter, obj);
        }
        return contentType;
    }
    
    public <T> void setRiakContentType(T obj, String contentType)
    {
        if (riakContentTypeField != null)
        {
            setFieldValue(riakContentTypeField, obj, contentType);
        } 
        else if (riakContentTypeSetter != null)
        {
            setMethodValue(riakContentTypeSetter, obj, contentType);
        }
    }
    
    public <T> void setRiakLastModified(T obj, Long lastModified)
    {
        if (riakLastModifiedField != null)
        {
            setFieldValue(riakLastModifiedField, obj, lastModified);
        }
        else if (riakLastModifiedSetter != null)
        {
            setMethodValue(riakLastModifiedSetter, obj, lastModified);
        }
    }
    
    public <T> void setRiakVTag(T obj, String vtag)
    {
        if (riakVTagField != null)
        {
            setFieldValue(riakVTagField, obj, vtag);
        }
        else if (riakVTagSetter != null)
        {
            setMethodValue(riakVTagSetter, obj, vtag);
        }
    }
    
    public <T> RiakUserMetadata getUsermetaData(RiakUserMetadata container, T obj)
    {
        for (UsermetaField uf : usermetaFields)
        {
            switch(uf.getFieldType())
            {
                case MAP:
                    @SuppressWarnings("unchecked")
                    Map<String, String> map = (Map<String, String>) getFieldValue(uf.getField(), obj);
                    if (map != null)
                    {
                        container.put(map);
                    }
                    break;
                case STRING:
                    Object o = getFieldValue(uf.getField(), obj);
                    String val = o == null ? null : o.toString();
                    String key = uf.getUsermetaDataKey();
                    // null is not a user meta datum
                    if (o != null)
                    {
                        container.put(key, val);
                    }
                    break;
                default:
                    break;
            }
        }

        for (UsermetaMethod um : usermetaMethods)
        {
            switch(um.getMethodType())
            {
                case MAP_GETTER:
                    @SuppressWarnings("unchecked")
                    Map<String, String> map = (Map<String, String>) getMethodValue(um.getMethod(), obj);
                    if (map != null)
                    {
                        container.put(map);
                    }
                    break;
                case STRING_GETTER:
                    Object o = getMethodValue(um.getMethod(), obj);
                    String val = o == null ? null : o.toString();
                    String key = um.getUsermetaDataKey();
                    if (o != null)
                    {
                        container.put(key, val);
                    }
                    break;
                default:
                    break;
            }
        }

        return container;
    }

    /**
     * Populates an @RiakUsermeta annotated domain object with the User metadata.
     * @param <T>
     * @param userMetadata
     * @param obj 
     */
    public <T> void setUsermetaData(RiakUserMetadata userMetadata, T obj)
    {
        Field mapField = null;
        for (UsermetaField uf : usermetaFields)
        {
            switch(uf.getFieldType())
            {
                case STRING:
                    if (userMetadata.containsKey(uf.getUsermetaDataKey()))
                    {
                        setFieldValue(uf.getField(), obj, userMetadata.get(uf.getUsermetaDataKey()));
                        userMetadata.remove(uf.getUsermetaDataKey());
                    }
                    break;
                case MAP:
                    mapField = uf.getField();
                    break;
                default:
                    break;
            }
        }

        Method mapSetter = null;
        for (UsermetaMethod um : usermetaMethods)
        {
            switch(um.getMethodType())
            {
                case STRING_SETTER:
                    if (userMetadata.containsKey(um.getUsermetaDataKey()))
                    {
                        setMethodValue(um.getMethod(), obj, userMetadata.get(um.getUsermetaDataKey()));
                        userMetadata.remove(um.getUsermetaDataKey());
                    }
                    break;
                case MAP_SETTER:
                    mapSetter = um.getMethod();
                    break;
                default:
                    break;
            }
        }

        
        if (mapSetter != null || mapField != null)
        {
            Map<String,String> mapCopy = new HashMap<String,String>(userMetadata.size());
            for (Map.Entry<BinaryValue,BinaryValue> entry : userMetadata.getUserMetadata())
            {
                mapCopy.put(entry.getKey().toString(), entry.getValue().toString());
            }
            
            if (mapSetter != null)
            {
                setMethodValue(mapSetter, obj, mapCopy);
            }

            if (mapField != null)
            {
                setFieldValue(mapField, obj, mapCopy);
            }
        }
    }

    /**
     * @param <T> The domain object type
     * @param obj the domain object
     * @return a {@link RiakIndexes} made of the values of the RiakIndex
     * annotated fields and methods.
     */
    @SuppressWarnings("unchecked")
    public <T> RiakIndexes getIndexes(RiakIndexes container, T obj)
    {
        for (RiakIndexField f : indexFields)
        {
            final Object val = getFieldValue(f.getField(), obj);
            switch(f.getFieldType())
            {
                case SET_LONG:
                case LONG:
                    // We want to create the index regardless
                    LongIntIndex longIndex = container.getIndex(LongIntIndex.named(f.getIndexName()));
                    if (val != null)
                    {
                        if (f.getFieldType() == RiakIndexField.FieldType.SET_LONG)
                        {
                            longIndex.add((Set<Long>) val);
                        }
                        else
                        {
                            longIndex.add((Long) val);
                        }
                    }
                    break;
                case SET_STRING:
                case STRING:
                    // We want to create the index regardless
                    StringBinIndex stringBinIndex = container.getIndex(StringBinIndex.named(f.getIndexName()));
                    if (val != null)
                    {
                        if (f.getFieldType() == RiakIndexField.FieldType.SET_STRING)
                        {
                            stringBinIndex.add((Set<String>) val);
                        }
                        else
                        {
                            stringBinIndex.add((String) val);
                        }
                    }
                    break;
                default:
                    break;
            }
            
        }

        for (RiakIndexMethod m : indexMethods)
        {
            Object val;
            
            switch (m.getMethodType())
            {
                case SET_LONG_GETTER:
                case LONG_GETTER:
                    val = getMethodValue(m.getMethod(), obj);
                    // We want to create the index regardless
                    LongIntIndex index = container.getIndex(LongIntIndex.named(m.getIndexName()));
                    if (val != null)
                    {
                        if (m.getMethodType() == RiakIndexMethod.MethodType.SET_LONG_GETTER)
                        {
                            index.add((Set<Long>) val);
                        }
                        else
                        {
                            index.add((Long) val);
                        }
                    }
                    break;
                case SET_STRING_GETTER:
                case STRING_GETTER:
                    val = getMethodValue(m.getMethod(), obj);
                    // We want to create the index regardless
                    StringBinIndex stringBinIndex = container.getIndex(StringBinIndex.named(m.getIndexName()));
                    if (val != null)
                    {
                        if (m.getMethodType() == RiakIndexMethod.MethodType.SET_STRING_GETTER)
                        {
                            stringBinIndex.add((Set<String>) val);
                        }
                        else
                        {
                            stringBinIndex.add((String) val);
                        }
                    }
                    break;
                default:
                    break;
            }
        }

        return container;
    }

    /**
     * @param <T>
     * @param indexes the RiakIndexes to copy to the domain object
     * @param obj the domain object to set indexes on
     */
    public <T> void setIndexes(RiakIndexes indexes, T obj)
    {
        // copy the index values to the correct fields
        for (RiakIndexField f : indexFields)
        {
            Set<?> val = null;
            switch(f.getFieldType())
            {
                case SET_LONG:
                case LONG:
                    LongIntIndex longIndex = indexes.getIndex(LongIntIndex.named(f.getIndexName()));
                    val = longIndex.values();
                    break;
                case SET_STRING:
                case STRING:
                    StringBinIndex stringIndex = indexes.getIndex(StringBinIndex.named(f.getIndexName()));
                    val = stringIndex.values();
                    break;
                default:
                    break;
            }
            
            if (val != null)
            {
                if (f.getFieldType() == RiakIndexField.FieldType.LONG || 
                      f.getFieldType() == RiakIndexField.FieldType.STRING) 
                {
                    if (!val.isEmpty())
                    {
                        setFieldValue(f.getField(), obj, val.iterator().next()); // take the first value
                    } 
                }
                else
                {
                    setFieldValue(f.getField(), obj, val);
                }
            }
        }

        for (RiakIndexMethod m : indexMethods)
        {
            Set<?> val = null;
            
            switch(m.getMethodType())
            {
                case SET_LONG_SETTER:
                case LONG_SETTER:
                    LongIntIndex longIndex = indexes.getIndex(LongIntIndex.named(m.getIndexName()));
                    val = longIndex.values();
                    break;
                case SET_STRING_SETTER:
                case STRING_SETTER:
                    StringBinIndex stringIndex = indexes.getIndex(StringBinIndex.named(m.getIndexName()));
                    val = stringIndex.values();
                    break;
                default:
                    break;
            }
            
            if (val != null)
            {
                if (m.getMethodType() == RiakIndexMethod.MethodType.LONG_SETTER ||
                      m.getMethodType() == RiakIndexMethod.MethodType.STRING_SETTER) 
                {
                    if (!val.isEmpty())
                    {
                        setMethodValue(m.getMethod(), obj, val.iterator().next()); // take the first value
                    } 
                }
                else
                {
                    setMethodValue(m.getMethod(), obj, val); 
                }

            }
        }
    }

    @SuppressWarnings("unchecked")
    public <T> RiakLinks getLinks(RiakLinks container, T obj)
    {
        Object o = null;
        if (riakLinksField != null)
        {
            o = getFieldValue(riakLinksField, obj);
        }
        else if (riakLinksGetter != null)
        {
            o = getMethodValue(riakLinksGetter, obj);
        }

        if (o != null)
        {
            container.addLinks((Collection<RiakLink>) o);
        }

        return container;
    }

    public <T> void setLinks(RiakLinks links, T obj)
    {
        if (riakLinksField != null)
        {
            setFieldValue(riakLinksField, obj, links.getLinks());
        }
        else if (riakLinksSetter != null)
        {
            setMethodValue(riakLinksSetter, obj, links.getLinks());
        }

    }

    public static class Builder
    {

        private Field riakKeyField;
        private Method riakKeySetter;
        private Method riakKeyGetter;
        private Field riakLinksField;
        private Method riakLinksGetter;
        private Method riakLinksSetter;
        private Field riakVClockField;
        private Method riakVClockSetter;
        private Method riakVClockGetter;
        private Field riakTombstoneField;
        private Method riakTombstoneSetter;
        private Method riakTombstoneGetter;
        private Field riakContentTypeField;
        private Method riakContentTypeGetter;
        private Method riakContentTypeSetter;
        private Field riakLastModifiedField;
        private Method riakLastModified;
        private Field riakVTagField;
        private Method riakVTagSetter;
        private Field riakBucketNameField;
        private Method riakBucketNameSetter;
        private Method riakBucketNameGetter;
        private Field riakBucketTypeField;
        private Method riakBucketTypeSetter;
        private Method riakBucketTypeGetter;
        private final List<UsermetaField> usermetaFields;
        private final List<UsermetaMethod> usermetaMethods;
        private final List<RiakIndexField> indexFields;
        private final List<RiakIndexMethod> indexMethods;
        

        /**
         * Constructs a builder for a new AnnotationInfo
         */
        public Builder()
        {
        }

        
        {
            usermetaFields = new LinkedList<UsermetaField>();
            usermetaMethods = new LinkedList<UsermetaMethod>();
            indexFields = new LinkedList<RiakIndexField>();
            indexMethods = new LinkedList<RiakIndexMethod>();
        }

        /**
         * Set the @RiakKey annotated field.
         *
         * @param f the annotated field
         * @return a reference to this object
         */
        public Builder withRiakKeyField(Field f)
        {
            validateStringOrByteField(f, riakKeyGetter, riakKeySetter, "@RiakKey");
            this.riakKeyField = ClassUtil.checkAndFixAccess(f);
            return this;
        }

        /**
         * Set the @RiakKey annotated getter.
         *
         * @param m the annotated method
         * @return a reference to this object
         */
        public Builder withRiakKeyGetter(Method m)
        {
            validateStringOrByteMethod(m, riakKeyField, "@RiakKey");
            this.riakKeyGetter = ClassUtil.checkAndFixAccess(m);
            return this;
        }

        /**
         * Set the @RiakKey annotated setter.
         *
         * @param m the annotated setter
         * @return a reference to this object
         */
        public Builder withRiakKeySetter(Method m)
        {
            validateStringOrByteMethod(m, riakKeyField, "@RiakKey");
            this.riakKeySetter = ClassUtil.checkAndFixAccess(m);
            return this;
        }

        /**
         * Set the @RiakLinks annotated field.
         *
         * @param f the annotated field
         * @return a reference to this object
         */
        public Builder withRiakLinksField(Field f)
        {
            validateRiakLinksField(f);
            this.riakLinksField = ClassUtil.checkAndFixAccess(f);
            return this;
        }

        /**
         * Set the @RiakLinks annotated getter.
         *
         * @param m the annotated method
         * @return a reference to this object
         */
        public Builder withRiakLinksGetter(Method m)
        {
            validateRiakLinksMethod(m);
            this.riakLinksGetter = ClassUtil.checkAndFixAccess(m);
            return this;
        }

        /**
         * Set the @RiakLinks annotated setter.
         *
         * @param m the annotated method
         * @return a reference to this object
         *
         */
        public Builder withRiakLinksSetter(Method m)
        {
            validateRiakLinksMethod(m);
            this.riakLinksSetter = ClassUtil.checkAndFixAccess(m);
            return this;
        }

        /**
         * Add a @RiakUsermeta annotated field.
         *
         * @param f the annotated field.
         * @return a reference to this object.
         */
        public Builder addRiakUsermetaField(Field f)
        {
            this.usermetaFields.add(new UsermetaField(ClassUtil.checkAndFixAccess(f)));
            return this;
        }

        /**
         * Add a @RiakUsermeta annotated method
         *
         * @param m the annotated method
         * @return a reference to this object
         */
        public Builder addRiakUsermetaMethod(Method m)
        {
            this.usermetaMethods.add(new UsermetaMethod(ClassUtil.checkAndFixAccess(m)));
            return this;
        }

        /**
         * Add a @RiakIndex annotated method.
         *
         * @param m the annotated method
         * @return a reference to this object
         */
        public Builder addRiakIndexMethod(Method m)
        {
            this.indexMethods.add(new RiakIndexMethod(ClassUtil.checkAndFixAccess(m)));
            return this;
        }

        /**
         * Add a @RiakIndex annotated field.
         *
         * @param f the annotated field
         * @return a reference to this object
         */
        public Builder addRiakIndexField(Field f)
        {
            this.indexFields.add(new RiakIndexField(ClassUtil.checkAndFixAccess(f)));
            return this;
        }

        /**
         * Set the @RiakVClock annotated field.
         *
         * @param f the annotated field
         * @return a reference to this object
         */
        public Builder withRiakVClockField(Field f)
        {
            validateVClockField(f);
            this.riakVClockField = ClassUtil.checkAndFixAccess(f);
            return this;
        }

        /**
         * Set the @RiakVClock annotated setter method.
         *
         * @param m the annotated method.
         * @return a reference to this object
         */
        public Builder withRiakVClockSetter(Method m)
        {
            validateVClockMethod(m);
            this.riakVClockSetter = ClassUtil.checkAndFixAccess(m);
            return this;
        }

        /**
         * Set the @RiakVClock annotated getter method.
         *
         * @param m the annotated method
         * @return a reference to this object.
         */
        public Builder withRiakVClockGetter(Method m)
        {
            validateVClockMethod(m);
            this.riakVClockGetter = ClassUtil.checkAndFixAccess(m);
            return this;
        }

        public Builder withRiakTombstoneField(Field f)
        {
            validateTombstoneField(f);
            this.riakTombstoneField = ClassUtil.checkAndFixAccess(f);
            return this;
        }

        public Builder withRiakTombstoneSetter(Method m)
        {
            validateTombstoneMethod(m);
            this.riakTombstoneSetter = ClassUtil.checkAndFixAccess(m);
            return this;
        }

        public Builder withRiakTombstoneGetter(Method m)
        {
            validateTombstoneMethod(m);
            this.riakTombstoneGetter = ClassUtil.checkAndFixAccess(m);
            return this;
        }
        
        public Builder withRiakContentTypeField(Field f)
        {
            validateContentTypeField(f);
            this.riakContentTypeField = ClassUtil.checkAndFixAccess(f);
            return this;
        }

        public Builder withRiakContentTypeSetter(Method m)
        {
            validateContentTypeMethod(m);
            this.riakContentTypeSetter = ClassUtil.checkAndFixAccess(m);
            return this;
        }
        
        public Builder withRiakContentTypeGetter(Method m)
        {
            validateContentTypeMethod(m);
            this.riakContentTypeGetter = ClassUtil.checkAndFixAccess(m);
            return this;
        }
        
        public Builder withRiakLastModifiedField(Field f)
        {
            validateLastModifiedField(f);
            this.riakLastModifiedField = ClassUtil.checkAndFixAccess(f);
            return this;
        }
        
        public Builder withRiakLastModifiedSetter(Method m)
        {
            validateLastModifiedMethod(m);
            this.riakLastModified = ClassUtil.checkAndFixAccess(m);
            return this;
        }
        
        public Builder withRiakVTagField(Field f)
        {
            validateVTagField(f);
            this.riakVTagField = ClassUtil.checkAndFixAccess(f);
            return this;
        }
        
        public Builder withRiakVTagSetter(Method m)
        {
            validateVTagMethod(m);
            this.riakVTagSetter = ClassUtil.checkAndFixAccess(m);
            return this;
        }
        
        public Builder withRiakBucketNameField(Field f)
        {
            validateStringOrByteField(f, riakBucketNameGetter, 
                                         riakBucketNameSetter, "@RiakBucketName");
            this.riakBucketNameField = ClassUtil.checkAndFixAccess(f);
            return this;
        }
        
        public Builder withRiakBucketNameSetter(Method m)
        {
            validateStringOrByteMethod(m, riakBucketNameField, "@RiakBucketName");
            this.riakBucketNameSetter = ClassUtil.checkAndFixAccess(m);
            return this;
        }
        
        public Builder withRiakBucketNameGetter(Method m)
        {
            validateStringOrByteMethod(m, riakBucketNameField, "@RiakBucketName");            
            this.riakBucketNameGetter = ClassUtil.checkAndFixAccess(m);
            return this;
        }
        
        public Builder withRiakBucketTypeField(Field f)
        {
            validateStringOrByteField(f, riakBucketTypeGetter, 
                                         riakBucketTypeSetter, "@RiakBucketType");
            this.riakBucketTypeField = ClassUtil.checkAndFixAccess(f);
            return this;
        }
        
        public Builder withRiakBucketTypeSetter(Method m)
        {
            validateStringOrByteMethod(m,riakBucketTypeField, "@RiakBucketType");
            this.riakBucketTypeSetter = ClassUtil.checkAndFixAccess(m);
            return this;
        }
        
        public Builder withRiakBucketTypeGetter(Method m)
        {
            validateStringOrByteMethod(m,riakBucketTypeField, "@RiakBucketType");
            this.riakBucketTypeGetter = ClassUtil.checkAndFixAccess(m);
            return this;
        }
        
        
        public AnnotationInfo build()
        {
            validateAnnotatedSet(riakVClockField, riakVClockGetter,
                                 riakVClockSetter, "@RiakVClock");
            validateAnnotatedSet(riakTombstoneField, riakTombstoneGetter,
                                 riakTombstoneSetter, "@RiakTombstone");
            validateAnnotatedSet(riakLinksField, riakLinksGetter,
                                 riakLinksSetter, "@RiakLinks");
            validateAnnotatedSet(riakKeyField, riakKeyGetter,
                                 riakKeySetter, "@RiakKey");
            validateAnnotatedSet(riakContentTypeField, riakContentTypeGetter,
                                 riakContentTypeSetter, "@RiakContentType");
            validateAnnotatedSet(riakBucketNameField, riakBucketNameGetter,
                                 riakBucketNameSetter, "@RiakBucketName");
            validateAnnotatedSet(riakBucketTypeField, riakBucketTypeGetter,
                                 riakBucketTypeSetter, "@RiakBucketType");
            

            return new AnnotationInfo(this);
        }

        private void validateAnnotatedSet(Field f, Method getter, Method setter, String annotation)
        {
            if (f == null && (getter == null || setter == null))
            {
                if (getter != null && setter == null)
                {
                    throw new IllegalStateException("Getter present for " + annotation + " without setter.");
                }
                else if (setter != null && getter == null)
                {
                    throw new IllegalStateException("Setter present for " + annotation + " without getter.");
                }
            }
        }

        private void validateRiakLinksField(Field riakLinksField)
        {

            if (riakLinksGetter != null || riakLinksSetter != null)
            {
                throw new IllegalArgumentException("@RiakLinks annotated method already set");
            }

            if (Collection.class.isAssignableFrom(riakLinksField.getType()))
            {
                Type t = riakLinksField.getGenericType();
                if (t instanceof ParameterizedType)
                {
                    ParameterizedType type = (ParameterizedType) t;
                    if (type.getRawType().equals(Collection.class))
                    {

                        Type[] genericParams = type.getActualTypeArguments();
                        if (genericParams.length == 1 && genericParams[0].equals(RiakLink.class))
                        {
                            return;
                        }
                    }
                }
            }
            else if (riakLinksField.getType().equals(RiakLinks.class))
            {
                return;
            }
            throw new IllegalArgumentException("@RiakLinks field must be Collection<RiakLink>");
        }

        private void validateRiakLinksMethod(Method m)
        {
            if (riakLinksField != null)
            {
                throw new IllegalArgumentException("@RiakLinks annotated field already set.");
            }
            if (m.getReturnType().equals(Void.TYPE))
            {
                // it's a setter, check the arg type
                Type[] genericParameterTypes = m.getGenericParameterTypes();
                Type t = genericParameterTypes[0];
                if (t instanceof ParameterizedType)
                {
                    ParameterizedType pType = (ParameterizedType) t;
                    if (pType.getRawType().equals(Collection.class))
                    {
                        Class<?> genericType = (Class<?>) pType.getActualTypeArguments()[0];
                        if (RiakLink.class.equals(genericType))
                        {
                            return;
                        }
                    }
                }

                throw new IllegalArgumentException("@RiakLinks setter must take Collection<RiakLink>");
            }
            else
            {
                // it's a getter, check return type
                Type t = m.getGenericReturnType();
                if (t instanceof ParameterizedType)
                {
                    ParameterizedType pType = (ParameterizedType) t;
                    if (pType.getRawType().equals(Collection.class))
                    {
                        Class<?> genericType = (Class<?>) pType.getActualTypeArguments()[0];
                        if (RiakLink.class.equals(genericType))
                        {
                            return;
                        }
                    }
                }
                throw new IllegalArgumentException("@RiakLinks getter must return Collection<RiakLink>");

            }
        }

        private void validateVClockField(Field f)
        {
            if (riakVClockGetter != null || riakVClockSetter != null)
            {
                throw new IllegalArgumentException("@RiakVClock annotated method already set.");
            }
            else if (!(f.getType().isArray() && f.getType().getComponentType().equals(byte.class))
                && !f.getType().isAssignableFrom(VClock.class))
            {
                throw new IllegalArgumentException("@RiakVClock field must be a VClock or byte[]");
            }
        }

        private void validateVClockMethod(Method m)
        {
            if (riakVClockField != null)
            {
                throw new IllegalArgumentException("@RiakVClock annotated field already set.");
            }
            else if (m.getReturnType().equals(Void.TYPE))
            {
                // It's a setter
                Class<?> pType = m.getParameterTypes()[0];
                if (!(pType.isArray() && pType.getComponentType().equals(byte.class))
                    && !pType.isAssignableFrom(VClock.class))
                {
                    throw new IllegalArgumentException("@RiakVClock setter must take VClock or byte[]");
                }
            }
            else
            {
                Class<?> rType = m.getReturnType();
                if (!(rType.isArray() && rType.getComponentType().equals(byte.class))
                    && !rType.isAssignableFrom(VClock.class))
                {
                    throw new IllegalArgumentException("@RiakVClock getter must return VClock or byte[]");
                }
            }

        }

        private void validateTombstoneField(Field f)
        {
            if (riakTombstoneGetter != null || riakTombstoneSetter != null)
            {
                throw new IllegalArgumentException("@RiakTombstone annotated method already set.");
            }
            else if (!f.getType().equals(Boolean.class) && !f.getType().equals(boolean.class))
            {
                throw new IllegalArgumentException("@RiakTombstone field must be Boolean or boolean");
            }
        }

        private void validateTombstoneMethod(Method m)
        {
            if (riakTombstoneField != null)
            {
                throw new IllegalArgumentException("@RiakTombstone annotated field already set");
            }
            else if (m.getReturnType().equals(Void.TYPE))
            {
                Class<?> pType = m.getParameterTypes()[0];
                if (!pType.equals(Boolean.class) && !pType.equals(boolean.class))
                {
                    throw new IllegalArgumentException("@RiakTombstone setter must take boolean or Boolean");
                }
            }
            else
            {
                Class<?> rType = m.getReturnType();
                if (!rType.equals(Boolean.class) && !rType.equals(boolean.class))
                {
                    throw new IllegalArgumentException("@RiakTombstone getter must return boolean or Boolean");
                }
            }
        }
        
        private void validateContentTypeField(Field f)
        {
            if (riakContentTypeGetter != null || riakContentTypeSetter != null)
            {
                throw new IllegalArgumentException("@RiakContentType annotated method already set.");
            }
            else if (!f.getType().equals(String.class))
            {
                throw new IllegalArgumentException("@RiakContentType field must be a String.");
            }
        }
        
        private void validateContentTypeMethod(Method m)
        {
            if (riakContentTypeField != null)
            {
                throw new IllegalArgumentException("@RiakContentType annotated field already set");
            }
            else if (m.getReturnType().equals(Void.TYPE))
            { 
                if (!String.class.equals(m.getParameterTypes()[0]))
                {
                    throw new IllegalArgumentException("@RiakContentType setter must take a String.");
                }
            }
            else
            {
                if (!m.getReturnType().equals(String.class))
                {
                    throw new IllegalArgumentException("@RiakContentType getter must return a String.");
                }
            }
        }
        
        private void validateLastModifiedField(Field f)
        {
            if (riakLastModified != null)
            {
                throw new IllegalArgumentException("@RiakLastModified annotated method already set.");
            }
            else if (!f.getType().equals(Long.class) && !f.getType().equals(long.class))
            {
                throw new IllegalArgumentException("@RiakLastModified field must be a Long or long.");
            }
        }
        
        private void validateLastModifiedMethod(Method m)
        {
            if (riakLastModifiedField != null)
            {
                throw new IllegalArgumentException("@RiakLastModified annotated field already set.");
            }
            else
            {
                if (!m.getReturnType().equals(Void.TYPE))
                {
                    throw new IllegalArgumentException("@RiakLastModified can only be applied to a setter method.");
                }
                else
                {
                    Class<?> pType = m.getParameterTypes()[0];
                    if (!pType.equals(Long.class) && !pType.equals(long.class))
                    {
                        throw new IllegalArgumentException("@RiakLastModified setter must take a long or Long.");
                    }
                }
            }
        }
        
        private void validateVTagField(Field f)
        {
            if (riakVTagSetter != null)
            {
                throw new IllegalArgumentException("@RiakVTag annotated method already set.");
            }
            else if (!f.getType().equals(String.class))
            {
                throw new IllegalArgumentException("@RiakVTag field must be a String.");
            }
        }
        
        private void validateVTagMethod(Method m)
        {
            if (riakVTagField != null)
            {
                throw new IllegalArgumentException("@RiakVTag annotated field already set.");
            }
            else
            {
                if (!m.getReturnType().equals(Void.TYPE))
                {
                    throw new IllegalArgumentException("@RiakVTag can only be applied to a setter method.");
                }
                else
                {
                    Class<?> pType = m.getParameterTypes()[0];
                    if (!pType.equals(String.class))
                    {
                        throw new IllegalArgumentException("@RiakVTag setter must take a String.");
                    }
                }
            }
        }
        
        private void validateStringOrByteField(Field f, Method getter, Method setter, String annotation)
        {
            if (getter != null || setter != null)
            {
                throw new IllegalArgumentException(annotation + " annotated method already set.");
            }
            else if (!(f.getType().isArray() && f.getType().getComponentType().equals(byte.class))
                && !f.getType().isAssignableFrom(String.class))
            {
                throw new IllegalArgumentException(annotation + " field must be a String or byte[].");
            }
        }
        
        private void validateStringOrByteMethod(Method m, Field field, String annotation)
        {
            if (field != null)
            {
                throw new IllegalArgumentException(annotation + " annotated field already set.");
            }
            else if (m.getReturnType().equals(Void.TYPE))
            {
                // It's a setter
                Class<?> pType = m.getParameterTypes()[0];
                if (!(pType.isArray() && pType.getComponentType().equals(byte.class))
                    && !pType.isAssignableFrom(String.class))
                {
                    throw new IllegalArgumentException(annotation + " setter must take String or byte[]");
                }
            }
            else
            {
                Class<?> rType = m.getReturnType();
                if (!(rType.isArray() && rType.getComponentType().equals(byte.class))
                    && !rType.isAssignableFrom(String.class))
                {
                    throw new IllegalArgumentException(annotation + " getter must return String or byte[]");
                }
            }
        }
    }
}

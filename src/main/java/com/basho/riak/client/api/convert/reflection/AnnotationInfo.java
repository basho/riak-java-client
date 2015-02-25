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
package com.basho.riak.client.api.convert.reflection;

import static com.basho.riak.client.api.convert.reflection.ClassUtil.getFieldValue;
import static com.basho.riak.client.api.convert.reflection.ClassUtil.setFieldValue;
import static com.basho.riak.client.api.convert.reflection.ClassUtil.getMethodValue;
import static com.basho.riak.client.api.convert.reflection.ClassUtil.setMethodValue;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.basho.riak.client.core.query.links.RiakLink;
import com.basho.riak.client.api.cap.BasicVClock;
import com.basho.riak.client.api.cap.VClock;
import com.basho.riak.client.core.query.UserMetadata.RiakUserMetadata;
import com.basho.riak.client.core.query.indexes.BigIntIndex;
import com.basho.riak.client.core.query.indexes.IndexType;
import com.basho.riak.client.core.query.indexes.LongIntIndex;
import com.basho.riak.client.core.query.indexes.RawIndex;
import com.basho.riak.client.core.query.indexes.RiakIndexes;
import com.basho.riak.client.core.query.indexes.StringBinIndex;
import com.basho.riak.client.core.query.links.RiakLinks;
import com.basho.riak.client.core.util.BinaryValue;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.util.HashSet;
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
        if (riakKeyGetter != null)
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
        else if (riakKeyField != null)
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
        
        return key;
    }

    // TODO: charset
    public <T> void setRiakKey(T obj, BinaryValue key)
    {
        if (riakKeySetter != null)
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
        else if (riakKeyField != null)
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
         
    }

    // TODO: charset in annotation
    public <T> BinaryValue getRiakBucketName(T obj)
    {
        BinaryValue bucketName = null;
        
        if (riakBucketNameGetter != null)
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
        else if (riakBucketNameField != null)
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
         
        return bucketName;
    }
    
    // TODO: charset in annotation
    public <T> void setRiakBucketName(T obj, BinaryValue bucketName)
    {
        if (riakBucketNameSetter != null)
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
        else if (riakBucketNameField != null)
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
    }
    
    // TODO: charset in annotation
    public <T> BinaryValue getRiakBucketType(T obj)
    {
        BinaryValue bucketType = null;
        
        if (riakBucketTypeGetter != null)
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
        else if (riakBucketTypeField != null)
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
        return bucketType;
    }
    
    // TODO: charset in annotation
    public <T> void setRiakBucketType(T obj, BinaryValue bucketType)
    {
        if (riakBucketTypeSetter != null)
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
        else if (riakBucketTypeField != null)
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
    }
    
    public boolean hasRiakVClock()
    {
        return riakVClockField != null || riakVClockSetter != null;
    }
    
    public <T> VClock getRiakVClock(T obj)
    {

        VClock vclock = null;

        // We allow the annotated field to be either an actual VClock, or
        // a byte array.
        if (riakVClockGetter != null)
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
        else if (riakVClockField != null)
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
        
        return vclock;
    }

    public <T> void setRiakVClock(T obj, VClock vclock)
    {

        // We allow the annotated field to be either an actual VClock, or
        // a byte array. This is enforced in the AnnotationScanner
        if (riakVClockSetter != null)
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
        else if (riakVClockField != null)
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
    }

    public <T> Boolean getRiakTombstone(T obj)
    {
        Boolean tombstone = null;
        if (riakTombstoneGetter != null)
        {
            tombstone = (Boolean) getMethodValue(riakTombstoneGetter, obj);
        }
        else if (riakTombstoneField != null)
        {
            tombstone = (Boolean) getFieldValue(riakTombstoneField, obj);
        }
        
        return tombstone;
    }

    public <T> void setRiakTombstone(T obj, Boolean isDeleted)
    {

        if (riakTombstoneSetter != null)
        {
            setMethodValue(riakTombstoneSetter, obj, isDeleted);
        }
        else if (riakTombstoneField != null)
        {
            setFieldValue(riakTombstoneField, obj, isDeleted);
        }
    }

    public <T> String getRiakContentType(T obj)
    {
        String contentType = null;
        if (riakContentTypeGetter != null)
        {
            contentType = (String) getMethodValue(riakContentTypeGetter, obj);
        }
        else if (riakContentTypeField != null)
        {
            contentType = (String) getFieldValue(riakContentTypeField, obj);
        }
        return contentType;
    }
    
    public <T> void setRiakContentType(T obj, String contentType)
    {
        if (riakContentTypeSetter != null)
        {
            setMethodValue(riakContentTypeSetter, obj, contentType);
        }
        else if (riakContentTypeField != null)
        {
            setFieldValue(riakContentTypeField, obj, contentType);
        } 
    }
    
    public <T> void setRiakLastModified(T obj, Long lastModified)
    {
        if (riakLastModifiedSetter != null)
        {
            setMethodValue(riakLastModifiedSetter, obj, lastModified);
        }
        else if (riakLastModifiedField != null)
        {
            setFieldValue(riakLastModifiedField, obj, lastModified);
        }
    }
    
    public <T> void setRiakVTag(T obj, String vtag)
    {
        if (riakVTagSetter != null)
        {
            setMethodValue(riakVTagSetter, obj, vtag);
        }
        else if (riakVTagField != null)
        {
            setFieldValue(riakVTagField, obj, vtag);
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
                case SET_BIG_INT:
                case BIG_INT:
                    BigIntIndex bigIntIndex = container.getIndex(BigIntIndex.named(f.getIndexName()));
                    if (val != null)
                    {
                        if (f.getFieldType() == RiakIndexField.FieldType.SET_BIG_INT)
                        {
                            bigIntIndex.add((Set<BigInteger>) val);
                        }
                        else
                        {
                            bigIntIndex.add((BigInteger) val);
                        }
                    }
                    break;
                case SET_RAW:
                case RAW:
                {
                    // We want to create the index regardless
                    IndexType iType = IndexType.typeFromFullname(f.getIndexName());
                    RawIndex rawIndex = container.getIndex(RawIndex.named(f.getIndexName(), iType));
                    if (val != null)
                    {
                        if (f.getFieldType() == RiakIndexField.FieldType.SET_RAW)
                        {
                            for (byte[] bytes : (Set<byte[]>) val)
                            {
                                rawIndex.add(BinaryValue.create(bytes));
                            }
                        }
                        else
                        {
                            rawIndex.add(BinaryValue.create((byte[])val));
                        }
                    }
                }
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
                case SET_BIG_INT_GETTER:
                case BIG_INT_GETTER:
                    val = getMethodValue(m.getMethod(), obj);
                    // We want to create the index regardless
                    BigIntIndex bigIntIndex = container.getIndex(BigIntIndex.named(m.getIndexName()));
                    if (val != null)
                    {
                        if (m.getMethodType() == RiakIndexMethod.MethodType.SET_BIG_INT_GETTER)
                        {
                            bigIntIndex.add((Set<BigInteger>) val);
                        }
                        else
                        {
                            bigIntIndex.add((BigInteger) val);
                        }
                    }
                    break;
                case SET_RAW_GETTER:
                case RAW_GETTER:
                    val = getMethodValue(m.getMethod(), obj);
                    IndexType iType = IndexType.typeFromFullname(m.getIndexName());
                    RawIndex rawIndex = container.getIndex(RawIndex.named(m.getIndexName(), iType));
                    if (val != null)
                    {
                        if (m.getMethodType() == RiakIndexMethod.MethodType.SET_RAW_GETTER)
                        {
                            for (byte[] bytes : (Set<byte[]>) val)
                            {
                                rawIndex.add(BinaryValue.create(bytes));
                            }
                        }
                        else
                        {
                            rawIndex.add(BinaryValue.create((byte[]) val));
                        }
                    }
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
                case SET_BIG_INT:
                case BIG_INT:
                    BigIntIndex bigIntIndex = indexes.getIndex(BigIntIndex.named(f.getIndexName()));
                    val = bigIntIndex.values();
                    break;
                case SET_RAW:
                case RAW:
                    IndexType iType = IndexType.typeFromFullname(f.getIndexName());
                    RawIndex rawIndex = indexes.getIndex((RawIndex.named(f.getIndexName(), iType)));
                    // Convert from BinaryValue to bytes
                    Set<byte[]> byteSet = new HashSet<byte[]>();
                    for (BinaryValue bv : rawIndex.values())
                    {
                        byteSet.add(bv.unsafeGetValue());
                    }
                    val = byteSet;
                    break;
                default:
                    break;
            }
            
            if (val != null)
            {
                if (f.getFieldType() == RiakIndexField.FieldType.LONG || 
                      f.getFieldType() == RiakIndexField.FieldType.STRING ||
                      f.getFieldType() == RiakIndexField.FieldType.BIG_INT ||
                      f.getFieldType() == RiakIndexField.FieldType.RAW) 
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
                case SET_BIG_INT_SETTER:
                case BIG_INT_SETTER:
                    BigIntIndex bigIntIndex = indexes.getIndex(BigIntIndex.named(m.getIndexName()));
                    val = bigIntIndex.values();
                    break;
                case SET_RAW_SETTER:
                case RAW_SETTER:
                    IndexType iType = IndexType.typeFromFullname(m.getIndexName());
                    RawIndex rawIndex = indexes.getIndex(RawIndex.named(m.getIndexName(), iType));
                    // Convert from BinaryValue to bytes
                    Set<byte[]> byteSet = new HashSet<byte[]>();
                    for (BinaryValue bv : rawIndex.values())
                    {
                        byteSet.add(bv.unsafeGetValue());
                    }
                    val = byteSet;
                    break;
                default:
                    break;
            }
            
            if (val != null)
            {
                if (m.getMethodType() == RiakIndexMethod.MethodType.LONG_SETTER ||
                      m.getMethodType() == RiakIndexMethod.MethodType.STRING_SETTER ||
                      m.getMethodType() == RiakIndexMethod.MethodType.BIG_INT_SETTER ||
                      m.getMethodType() == RiakIndexMethod.MethodType.RAW_SETTER) 
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
        if (riakLinksGetter != null)
        {
            o = getMethodValue(riakLinksGetter, obj);
        }
        else if (riakLinksField != null)
        {
            o = getFieldValue(riakLinksField, obj);
        }
        
        if (o != null)
        {
            container.addLinks((Collection<RiakLink>) o);
        }

        return container;
    }

    public <T> void setLinks(RiakLinks links, T obj)
    {
        if (riakLinksSetter != null)
        {
            setMethodValue(riakLinksSetter, obj, links.getLinks());
        }
        else if (riakLinksField != null)
        {
            setFieldValue(riakLinksField, obj, links.getLinks());
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
            validateStringOrByteField(f, "@RiakKey");
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
            validateStringOrByteMethod(m, "@RiakKey");
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
            validateStringOrByteMethod(m, "@RiakKey");
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
            validateStringOrByteField(f, "@RiakBucketName");
            this.riakBucketNameField = ClassUtil.checkAndFixAccess(f);
            return this;
        }
        
        public Builder withRiakBucketNameSetter(Method m)
        {
            validateStringOrByteMethod(m, "@RiakBucketName");
            this.riakBucketNameSetter = ClassUtil.checkAndFixAccess(m);
            return this;
        }
        
        public Builder withRiakBucketNameGetter(Method m)
        {
            validateStringOrByteMethod(m, "@RiakBucketName");            
            this.riakBucketNameGetter = ClassUtil.checkAndFixAccess(m);
            return this;
        }
        
        public Builder withRiakBucketTypeField(Field f)
        {
            validateStringOrByteField(f, "@RiakBucketType");
            this.riakBucketTypeField = ClassUtil.checkAndFixAccess(f);
            return this;
        }
        
        public Builder withRiakBucketTypeSetter(Method m)
        {
            validateStringOrByteMethod(m, "@RiakBucketType");
            this.riakBucketTypeSetter = ClassUtil.checkAndFixAccess(m);
            return this;
        }
        
        public Builder withRiakBucketTypeGetter(Method m)
        {
            validateStringOrByteMethod(m, "@RiakBucketType");
            this.riakBucketTypeGetter = ClassUtil.checkAndFixAccess(m);
            return this;
        }
        
        
        public AnnotationInfo build()
        {
            return new AnnotationInfo(this);
        }

        private void validateRiakLinksField(Field riakLinksField)
        {

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
            if (m.getParameterTypes().length == 1)
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
            else if (m.getParameterTypes().length == 0)
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
            if (!(f.getType().isArray() && f.getType().getComponentType().equals(byte.class))
                && !f.getType().isAssignableFrom(VClock.class))
            {
                throw new IllegalArgumentException("@RiakVClock field must be a VClock or byte[]");
            }
        }

        private void validateVClockMethod(Method m)
        {
            if (m.getParameterTypes().length == 1)
            {
                // It's a setter
                Class<?> pType = m.getParameterTypes()[0];
                if (!(pType.isArray() && pType.getComponentType().equals(byte.class))
                    && !pType.isAssignableFrom(VClock.class))
                {
                    throw new IllegalArgumentException("@RiakVClock setter must take VClock or byte[]");
                }
            }
            else if (m.getParameterTypes().length == 0)
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
            if (!f.getType().equals(Boolean.class) && !f.getType().equals(boolean.class))
            {
                throw new IllegalArgumentException("@RiakTombstone field must be Boolean or boolean");
            }
        }

        private void validateTombstoneMethod(Method m)
        {
            if (m.getParameterTypes().length == 1)
            {
                Class<?> pType = m.getParameterTypes()[0];
                if (!pType.equals(Boolean.class) && !pType.equals(boolean.class))
                {
                    throw new IllegalArgumentException("@RiakTombstone setter must take boolean or Boolean");
                }
            }
            else if (m.getParameterTypes().length == 0)
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
            if (!f.getType().equals(String.class))
            {
                throw new IllegalArgumentException("@RiakContentType field must be a String.");
            }
        }
        
        private void validateContentTypeMethod(Method m)
        {
            if (m.getParameterTypes().length == 1)
            { 
                if (!String.class.equals(m.getParameterTypes()[0]))
                {
                    throw new IllegalArgumentException("@RiakContentType setter must take a String.");
                }
            }
            else if (m.getParameterTypes().length == 0)
            {
                if (!m.getReturnType().equals(String.class))
                {
                    throw new IllegalArgumentException("@RiakContentType getter must return a String.");
                }
            }
        }
        
        private void validateLastModifiedField(Field f)
        {
            if (!f.getType().equals(Long.class) && !f.getType().equals(long.class))
            {
                throw new IllegalArgumentException("@RiakLastModified field must be a Long or long.");
            }
        }
        
        private void validateLastModifiedMethod(Method m)
        {
            if (m.getParameterTypes().length == 0)
            {
                throw new IllegalArgumentException("@RiakLastModified can only be applied to a setter method.");
            }
            else if (m.getParameterTypes().length == 1)
            {
                Class<?> pType = m.getParameterTypes()[0];
                if (!pType.equals(Long.class) && !pType.equals(long.class))
                {
                    throw new IllegalArgumentException("@RiakLastModified setter must take a long or Long.");
                }
            }
        }
        
        private void validateVTagField(Field f)
        {
            if (!f.getType().equals(String.class))
            {
                throw new IllegalArgumentException("@RiakVTag field must be a String.");
            }
        }
        
        private void validateVTagMethod(Method m)
        {
            if (m.getParameterTypes().length == 0)
            {
                throw new IllegalArgumentException("@RiakVTag can only be applied to a setter method.");
            }
            else if (m.getParameterTypes().length == 1)
            {
                Class<?> pType = m.getParameterTypes()[0];
                if (!pType.equals(String.class))
                {
                    throw new IllegalArgumentException("@RiakVTag setter must take a String.");
                }
            }
        }
        
        private void validateStringOrByteField(Field f, String annotation)
        {
            if (!(f.getType().isArray() && f.getType().getComponentType().equals(byte.class))
                && !f.getType().isAssignableFrom(String.class))
            {
                throw new IllegalArgumentException(annotation + " field must be a String or byte[].");
            }
        }
        
        private void validateStringOrByteMethod(Method m, String annotation)
        {
            if (m.getParameterTypes().length == 1)
            {
                // It's a setter
                Class<?> pType = m.getParameterTypes()[0];
                if (!(pType.isArray() && pType.getComponentType().equals(byte.class))
                    && !pType.isAssignableFrom(String.class))
                {
                    throw new IllegalArgumentException(annotation + " setter must take String or byte[]");
                }
            }
            else if (m.getParameterTypes().length == 0)
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

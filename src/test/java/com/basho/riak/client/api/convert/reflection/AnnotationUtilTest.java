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

import com.basho.riak.client.api.convert.reflection.AnnotationUtil;
import com.basho.riak.client.api.annotations.RiakBucketName;
import com.basho.riak.client.api.annotations.RiakContentType;
import com.basho.riak.client.api.annotations.RiakKey;
import com.basho.riak.client.api.annotations.RiakTombstone;
import com.basho.riak.client.api.annotations.RiakLastModified;
import com.basho.riak.client.api.annotations.RiakVClock;
import com.basho.riak.client.api.annotations.RiakLinks;
import com.basho.riak.client.api.annotations.RiakVTag;
import com.basho.riak.client.api.annotations.RiakBucketType;
import com.basho.riak.client.api.annotations.RiakIndex;
import com.basho.riak.client.api.annotations.RiakUsermeta;
import com.basho.riak.client.api.cap.BasicVClock;
import com.basho.riak.client.api.cap.VClock;
import com.basho.riak.client.core.query.UserMetadata.RiakUserMetadata;
import com.basho.riak.client.core.query.indexes.IndexType;
import com.basho.riak.client.core.query.indexes.LongIntIndex;
import com.basho.riak.client.core.query.indexes.RawIndex;
import com.basho.riak.client.core.query.indexes.RiakIndexes;
import com.basho.riak.client.core.query.indexes.StringBinIndex;
import com.basho.riak.client.core.query.links.RiakLink;
import com.basho.riak.client.core.util.BinaryValue;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class AnnotationUtilTest
{
    @Test
    public void getKeyStringField()
    {
        final String expected = "aKey";
        final PojoWithAnnotatedFields pojo = new PojoWithAnnotatedFields();
        pojo.key = expected;

        assertEquals(BinaryValue.create(expected), AnnotationUtil.getKey(pojo));
        assertEquals(BinaryValue.create(expected), AnnotationUtil.getKey(pojo, null));
    }
    
    @Test 
    public void getKeyByteField()
    {
        final byte[] expected = "aKey".getBytes();
        final PojoWithAnnotatedByteFields pojo = new PojoWithAnnotatedByteFields();
        pojo.key = expected;
        
        assertEquals(BinaryValue.create(expected), AnnotationUtil.getKey(pojo));
        assertEquals(BinaryValue.create(expected), AnnotationUtil.getKey(pojo, null));
    }
        
    
    @Test
    public void setKeyStringField()
    {
        final BinaryValue expected = BinaryValue.create("aKey");
        final PojoWithAnnotatedFields pojo = new PojoWithAnnotatedFields();
        AnnotationUtil.setKey(pojo, expected);
        
        assertNotNull(pojo.key);
        assertEquals(expected.toString(), pojo.key);
    }

    @Test
    public void setKeyByteField()
    {
        final BinaryValue expected = BinaryValue.create("aKey");
        final PojoWithAnnotatedByteFields pojo = new PojoWithAnnotatedByteFields();
        AnnotationUtil.setKey(pojo, expected);
        
        assertNotNull(pojo.key);
        assertArrayEquals(expected.getValue(), pojo.key);
    }
    
    @Test
    public void getKeyStringMethod()
    {
        final String expected = "aKey";
        final PojoWithAnnotatedMethods pojo = new PojoWithAnnotatedMethods();
        pojo.setKey(expected);
        
        assertEquals(BinaryValue.create(expected), AnnotationUtil.getKey(pojo));
        assertEquals(BinaryValue.create(expected), AnnotationUtil.getKey(pojo, null));
    }
    
    @Test
    public void getKeyByteMethod()
    {
        final byte[] expected = "aKey".getBytes();
        final PojoWithAnnotatedByteMethods pojo = new PojoWithAnnotatedByteMethods();
        pojo.setKey(expected);
        
        assertEquals(BinaryValue.create(expected), AnnotationUtil.getKey(pojo));
        assertEquals(BinaryValue.create(expected), AnnotationUtil.getKey(pojo, null));
    }
    
    @Test 
    public void setKeyByteMethod()
    {
        final byte[] expected = "aKey".getBytes();
        final PojoWithAnnotatedByteMethods pojo = new PojoWithAnnotatedByteMethods();
        AnnotationUtil.setKey(pojo, BinaryValue.create(expected));
        
        assertNotNull(pojo.getKey());
        assertArrayEquals(expected, pojo.getKey());
    }
    
    @Test
    public void setKeyStringMethod()
    {
        final BinaryValue expected = BinaryValue.create("aKey");
        final PojoWithAnnotatedMethods pojo = new PojoWithAnnotatedMethods();
        AnnotationUtil.setKey(pojo, expected);
        
        assertNotNull(pojo.getKey());
        assertEquals(expected.toString(), pojo.getKey());
    }
    
    @Test(expected = RuntimeException.class)
    public void getNonStringKeyField()
    {
        final Object o = new Object() {
            @RiakKey 
            Date key;
        };
        
        AnnotationUtil.getKey(o);
    }

    @Test(expected = RuntimeException.class)
    public void getNonStringKeyGetter()
    {
        final Object o = new Object() {
            @RiakKey 
            Date getKey() { return null; }
            
            @RiakKey
            void setKey(String key) {}
            
        };
        
        AnnotationUtil.getKey(o);
    }
    
    @Test(expected = RuntimeException.class)
    public void getNonStringKeySetter()
    {
        final Object o = new Object() {
            @RiakKey 
            String getKey() { return null; }
            
            @RiakKey
            void setKey(Date key) {}
            
        };
        
        AnnotationUtil.setKey(o, BinaryValue.create("Some Date"));
    }
    
    @Test
    public void noKeyFieldOrMethod()
    {
        final Object o = new Object();

        assertNull(AnnotationUtil.getKey(o));
        assertNotNull(AnnotationUtil.getKey(o, BinaryValue.create("default")));
        AnnotationUtil.setKey(o, BinaryValue.create("default")); // should do nothing
    }

    @Test
    public void getNullKeyField()
    {
        final String expected = null;
        final PojoWithAnnotatedFields pojo = new PojoWithAnnotatedFields();
        pojo.key = expected;

        assertNull(AnnotationUtil.getKey(pojo));
        assertNotNull(AnnotationUtil.getKey(pojo, BinaryValue.create("default")));
    }
    
    @Test
    public void nullKeyMethod()
    {
        final String expected = null;
        final PojoWithAnnotatedMethods pojo = new PojoWithAnnotatedMethods();
        pojo.setKey(expected);

        assertNull(AnnotationUtil.getKey(pojo));
        assertNotNull(AnnotationUtil.getKey(pojo, BinaryValue.create("default")));
    }

    @Test
    public void getBucketNameStringField()
    {
        final String expected = "aBucket";
        final PojoWithAnnotatedFields pojo = new PojoWithAnnotatedFields();
        pojo.bucketName = expected;

        assertEquals(BinaryValue.create(expected), AnnotationUtil.getBucketName(pojo));
        assertEquals(BinaryValue.create(expected), AnnotationUtil.getBucketName(pojo, null));
    }
    
    @Test
    public void getBucketNameByteField()
    {
        byte[] expected = "aBucket".getBytes();
        final PojoWithAnnotatedByteFields pojo = new PojoWithAnnotatedByteFields();
        pojo.bucketName = expected;
        
        assertEquals(BinaryValue.create(expected), AnnotationUtil.getBucketName(pojo));
        assertEquals(BinaryValue.create(expected), AnnotationUtil.getBucketName(pojo, null));
        
    }
    
    @Test
    public void setBucketNameStringField()
    {
        final BinaryValue expected = BinaryValue.create("aBucket");
        final PojoWithAnnotatedFields pojo = new PojoWithAnnotatedFields();
        AnnotationUtil.setBucketName(pojo, expected);
        
        assertNotNull(pojo.bucketName);
        assertEquals(expected.toString(), pojo.bucketName);
    }

    @Test
    public void setBucketNameByteField()
    {
        final BinaryValue expected = BinaryValue.create("aBucket");
        final PojoWithAnnotatedByteFields pojo = new PojoWithAnnotatedByteFields();
        AnnotationUtil.setBucketName(pojo, expected);
        
        assertNotNull(pojo.bucketName);
        assertArrayEquals(expected.getValue(), pojo.bucketName);
    }
    
    @Test
    public void getBucketNameStringMethod()
    {
        final String expected = "aBucket";
        final PojoWithAnnotatedMethods pojo = new PojoWithAnnotatedMethods();
        pojo.setBucketName(expected);
        
        assertEquals(BinaryValue.create(expected), AnnotationUtil.getBucketName(pojo));
        assertEquals(BinaryValue.create(expected), AnnotationUtil.getBucketName(pojo, null));
    }
    
    @Test
    public void getBucketNameByteMethod()
    {
        byte[] expected = "aBucket".getBytes();
        final PojoWithAnnotatedByteMethods pojo = new PojoWithAnnotatedByteMethods();
        pojo.setBucketName(expected);
        
        assertEquals(BinaryValue.create(expected), AnnotationUtil.getBucketName(pojo));
        assertEquals(BinaryValue.create(expected), AnnotationUtil.getBucketName(pojo, null));
    }
    
    @Test
    public void setBucketNameStringMethod()
    {
        final BinaryValue expected = BinaryValue.create("aBucket");
        final PojoWithAnnotatedMethods pojo = new PojoWithAnnotatedMethods();
        AnnotationUtil.setBucketName(pojo, expected);
        
        assertNotNull(pojo.bucketName);
        assertEquals(expected.toString(), pojo.bucketName);
    }
    
    @Test
    public void setBucketNameByteMethod()
    {
        byte[] expected = "aBucket".getBytes();
        final PojoWithAnnotatedByteMethods pojo = new PojoWithAnnotatedByteMethods();
        AnnotationUtil.setBucketName(pojo, BinaryValue.create(expected));
        
        assertNotNull(pojo.bucketName);
        assertArrayEquals(expected, pojo.bucketName);

    }
    
    @Test(expected = RuntimeException.class)
    public void getIllegalBucketNameField()
    {
        final Object o = new Object()
        {
            @RiakBucketName
            Date bucketName;
        };
        
        AnnotationUtil.getBucketName(o);
    }

    @Test(expected = RuntimeException.class)
    public void illegalBucketNameGetter()
    {
        final Object o = new Object()
        {
            @RiakBucketName 
            Date getKey()
            {
                return null;
            }
            
            @RiakBucketName
            void setKey(String key)
            {
            }
            
        };
        
        AnnotationUtil.getBucketName(o);
    }
    
    @Test(expected = RuntimeException.class)
    public void illegalBucketNameSetter()
    {
        final Object o = new Object() {
            @RiakBucketName 
            String getBucketName()
            {
                return null;
            }
            
            @RiakBucketName
            void setBucketName(Date key)
            {
            }
        };
        
        AnnotationUtil.setBucketName(o, BinaryValue.create("Some Date"));
    }
    
    @Test
    public void noBucketNameFieldOrMethod()
    {
        final Object o = new Object()
        {
            private final String bucketName = "tomatoes";
        };

        assertNull(AnnotationUtil.getBucketName(o));
        assertNotNull(AnnotationUtil.getBucketName(o, BinaryValue.create("default")));
        AnnotationUtil.setBucketName(o, BinaryValue.create("default")); // should do nothing
    }

    @Test
    public void nullBucketNameField()
    {
        final String expected = null;
        final PojoWithAnnotatedFields pojo = new PojoWithAnnotatedFields();
        pojo.bucketName = expected;

        assertNull(AnnotationUtil.getBucketName(pojo));
        assertNotNull(AnnotationUtil.getBucketName(pojo, BinaryValue.create("default")));
    }
    
    @Test
    public void nullBucketNameMethod()
    {
        final String expected = null;
        final PojoWithAnnotatedMethods pojo = new PojoWithAnnotatedMethods();
        pojo.setBucketName(expected);

        assertNull(AnnotationUtil.getBucketName(pojo));
        assertNotNull(AnnotationUtil.getBucketName(pojo, BinaryValue.create("default")));
    }
    
    @Test
    public void getBucketTypeStringField()
    {
        final String expected = "aBucketType";
        final PojoWithAnnotatedFields pojo = new PojoWithAnnotatedFields();
        pojo.bucketType = expected;

        assertEquals(BinaryValue.create(expected), AnnotationUtil.getBucketType(pojo));
        assertEquals(BinaryValue.create(expected), AnnotationUtil.getBucketType(pojo, null));
    }
    
    @Test
    public void getBucketTypeByteField()
    {
        byte[] expected = "aBucketType".getBytes();
        final PojoWithAnnotatedByteFields pojo = new PojoWithAnnotatedByteFields();
        pojo.bucketType = expected;
        
        assertEquals(BinaryValue.create(expected), AnnotationUtil.getBucketType(pojo));
        assertEquals(BinaryValue.create(expected), AnnotationUtil.getBucketType(pojo, null));
        
    }
    
    @Test
    public void setBucketTypeStringField()
    {
        final BinaryValue expected = BinaryValue.create("aBucketType");
        final PojoWithAnnotatedFields pojo = new PojoWithAnnotatedFields();
        AnnotationUtil.setBucketType(pojo, expected);
        
        assertNotNull(pojo.bucketType);
        assertEquals(expected.toString(), pojo.bucketType);
    }

    @Test
    public void setBucketTypeByteField()
    {
        final BinaryValue expected = BinaryValue.create("aBucketType");
        final PojoWithAnnotatedByteFields pojo = new PojoWithAnnotatedByteFields();
        AnnotationUtil.setBucketType(pojo, expected);
        
        assertNotNull(pojo.bucketType);
        assertArrayEquals(expected.getValue(), pojo.bucketType);
    }
    
    @Test
    public void getBucketTypeStringMethod()
    {
        final String expected = "aBucketType";
        final PojoWithAnnotatedMethods pojo = new PojoWithAnnotatedMethods();
        pojo.setBucketType(expected);
        
        assertEquals(BinaryValue.create(expected), AnnotationUtil.getBucketType(pojo));
        assertEquals(BinaryValue.create(expected), AnnotationUtil.getBucketType(pojo, null));
    }
    
    @Test
    public void getBucketTypeByteMethod()
    {
        byte[] expected = "aBucketType".getBytes();
        final PojoWithAnnotatedByteMethods pojo = new PojoWithAnnotatedByteMethods();
        pojo.setBucketType(expected);
        
        assertEquals(BinaryValue.create(expected), AnnotationUtil.getBucketType(pojo));
        assertEquals(BinaryValue.create(expected), AnnotationUtil.getBucketType(pojo, null));
    }
    
    @Test
    public void setBucketTypeStringMethod()
    {
        final BinaryValue expected = BinaryValue.create("aBucketType");
        final PojoWithAnnotatedMethods pojo = new PojoWithAnnotatedMethods();
        AnnotationUtil.setBucketType(pojo, expected);
        
        assertNotNull(pojo.bucketType);
        assertEquals(expected.toString(), pojo.bucketType);
    }
    
    @Test
    public void setBucketTypeByteMethod()
    {
        byte[] expected = "aBucketType".getBytes();
        final PojoWithAnnotatedByteMethods pojo = new PojoWithAnnotatedByteMethods();
        AnnotationUtil.setBucketType(pojo, BinaryValue.create(expected));
        
        assertNotNull(pojo.bucketType);
        assertArrayEquals(expected, pojo.bucketType);

    }
    
    @Test(expected = RuntimeException.class)
    public void getIllegalBucketTypeField()
    {
        final Object o = new Object() {
            @RiakBucketType
            Date bucketType;
        };
        
        AnnotationUtil.getBucketType(o);
    }

    @Test(expected = RuntimeException.class)
    public void illegalBucketTypeGetter()
    {
        final Object o = new Object() {
            @RiakBucketType 
            Date getType() { return null; }
            
            @RiakBucketType
            void setType(String key) {}
            
        };
        
        AnnotationUtil.getBucketType(o);
    }
    
    @Test(expected = RuntimeException.class)
    public void illegalBucketTypeSetter()
    {
        final Object o = new Object() {
            @RiakBucketType 
            String getBucketType() { return null; }
            
            @RiakBucketType
            void setBucketType(Date key) {}
            
        };
        
        AnnotationUtil.setBucketType(o, BinaryValue.create("Some Date"));
    }
    
    @Test
    public void noBucketTypeFieldOrMethod()
    {
        final Object o = new Object()
        {
            private final String bucketType = "tomatoes";
        };

        assertNull(AnnotationUtil.getBucketType(o));
        assertNotNull(AnnotationUtil.getBucketType(o, BinaryValue.create("default")));
        AnnotationUtil.setBucketType(o, BinaryValue.create("default")); // should do nothing
    }

    @Test
    public void nullBucketTypeField()
    {
        final String expected = null;
        final PojoWithAnnotatedFields pojo = new PojoWithAnnotatedFields();
        pojo.bucketType = expected;

        assertNull(AnnotationUtil.getBucketType(pojo));
        assertNotNull(AnnotationUtil.getBucketType(pojo, BinaryValue.create("default")));
    }
    
    @Test
    public void nullBucketTypeMethod()
    {
        final String expected = null;
        final PojoWithAnnotatedMethods pojo = new PojoWithAnnotatedMethods();
        pojo.setBucketType(expected);

        assertNull(AnnotationUtil.getBucketType(pojo));
        assertNotNull(AnnotationUtil.getBucketType(pojo, BinaryValue.create("default")));
    }

    @Test
    public void getVClockField()
    {
        final byte[] clockBytes = "a85hYGBgzGDKBVIcypz/fvo/2e2UwZTImMfKwHIy/SRfFgA=".getBytes();
        final VClock expected = new BasicVClock(clockBytes);
        final PojoWithAnnotatedFields pojo = new PojoWithAnnotatedFields();
        pojo.vclock = expected;

        assertEquals(expected, AnnotationUtil.getVClock(pojo));
        assertEquals(expected, AnnotationUtil.getVClock(pojo, null));
    }
    
    @Test
    public void getVClockByteField()
    {
        final byte[] expected = "a85hYGBgzGDKBVIcypz/fvo/2e2UwZTImMfKwHIy/SRfFgA=".getBytes();    
        final PojoWithAnnotatedByteFields pojo = new PojoWithAnnotatedByteFields();
        pojo.vclock = expected;
        
        assertEquals(new BasicVClock(expected), AnnotationUtil.getVClock(pojo));
        assertEquals(new BasicVClock(expected), AnnotationUtil.getVClock(pojo, null));
        
    }
    
    @Test
    public void setVClockField()
    {
        final byte[] clockBytes = "a85hYGBgzGDKBVIcypz/fvo/2e2UwZTImMfKwHIy/SRfFgA=".getBytes();
        final VClock expected = new BasicVClock(clockBytes);
        final PojoWithAnnotatedFields pojo = new PojoWithAnnotatedFields();
        AnnotationUtil.setVClock(pojo, expected);
        
        assertNotNull(pojo.vclock);
        assertEquals(expected, pojo.vclock);
    }

    @Test
    public void setVClockByteField()
    {
        final byte[] expected = "a85hYGBgzGDKBVIcypz/fvo/2e2UwZTImMfKwHIy/SRfFgA=".getBytes();
        final PojoWithAnnotatedByteFields pojo = new PojoWithAnnotatedByteFields();
        AnnotationUtil.setVClock(pojo, new BasicVClock(expected));
        
        assertNotNull(pojo.vclock);
        assertArrayEquals(expected, pojo.vclock);
    }
    
    @Test
    public void getVClockMethod()
    {
        final byte[] clockBytes = "a85hYGBgzGDKBVIcypz/fvo/2e2UwZTImMfKwHIy/SRfFgA=".getBytes();
        final VClock expected = new BasicVClock(clockBytes);
        final PojoWithAnnotatedMethods pojo = new PojoWithAnnotatedMethods();
        pojo.setVClock(expected);
        
        assertEquals(expected, AnnotationUtil.getVClock(pojo));
        assertEquals(expected, AnnotationUtil.getVClock(pojo, null));
    }
    
    @Test
    public void getVClockByteMethod()
    {
        final byte[] clockBytes = "a85hYGBgzGDKBVIcypz/fvo/2e2UwZTImMfKwHIy/SRfFgA=".getBytes();
        final VClock expected = new BasicVClock(clockBytes);
        final PojoWithAnnotatedByteMethods pojo = new PojoWithAnnotatedByteMethods();
        pojo.setVClock(clockBytes);
        
        assertEquals(expected, AnnotationUtil.getVClock(pojo));
        assertEquals(expected, AnnotationUtil.getVClock(pojo, null));
    }
    
    @Test
    public void setVClockMethod()
    {
        final byte[] clockBytes = "a85hYGBgzGDKBVIcypz/fvo/2e2UwZTImMfKwHIy/SRfFgA=".getBytes();
        final VClock expected = new BasicVClock(clockBytes);
        final PojoWithAnnotatedMethods pojo = new PojoWithAnnotatedMethods();
        AnnotationUtil.setVClock(pojo, expected);
        
        assertNotNull(pojo.vClock);
        assertEquals(expected, pojo.vClock);
    }
    
    @Test
    public void setVClockByteMethod()
    {
        final byte[] clockBytes = "a85hYGBgzGDKBVIcypz/fvo/2e2UwZTImMfKwHIy/SRfFgA=".getBytes();
        final VClock expected = new BasicVClock(clockBytes);
        final PojoWithAnnotatedByteMethods pojo = new PojoWithAnnotatedByteMethods();
        AnnotationUtil.setVClock(pojo, expected);
        
        assertNotNull(pojo.vclock);
        assertArrayEquals(clockBytes, pojo.vclock);

    }
    
    @Test(expected = RuntimeException.class)
    public void getIllegalVClockField()
    {
        final Object o = new Object() {
            @RiakVClock
            Date vclock;
        };
        
        AnnotationUtil.getVClock(o);
    }

    @Test(expected = RuntimeException.class)
    public void illegalVClockGetter()
    {
        final Object o = new Object() {
            @RiakVClock 
            Date getVClock() { return null; }
            
            @RiakVClock
            void setVClock(VClock key) {}
            
        };
        
        AnnotationUtil.getVClock(o);
    }
    
    @Test(expected = RuntimeException.class)
    public void illegalVClockSetter()
    {
        final Object o = new Object() {
            @RiakVClock 
            VClock getVClock() { return null; }
            
            @RiakVClock
            void setVClock(Date key) {}
            
        };
        final byte[] clockBytes = "a85hYGBgzGDKBVIcypz/fvo/2e2UwZTImMfKwHIy/SRfFgA=".getBytes();
        final VClock expected = new BasicVClock(clockBytes);
        AnnotationUtil.setVClock(o, expected);
    }
    
    @Test
    public void noVClockFieldOrMethod()
    {
        final Object o = new Object();

        final byte[] clockBytes = "a85hYGBgzGDKBVIcypz/fvo/2e2UwZTImMfKwHIy/SRfFgA=".getBytes();
        final VClock expected = new BasicVClock(clockBytes);
        assertNull(AnnotationUtil.getVClock(o));
        assertNotNull(AnnotationUtil.getVClock(o, expected));
        AnnotationUtil.setVClock(o, expected); // should do nothing
    }

    @Test
    public void nullVClockField()
    {
        final VClock expected = null;
        final PojoWithAnnotatedFields pojo = new PojoWithAnnotatedFields();
        pojo.vclock = expected;

        final byte[] clockBytes = "a85hYGBgzGDKBVIcypz/fvo/2e2UwZTImMfKwHIy/SRfFgA=".getBytes();
        assertNull(AnnotationUtil.getVClock(pojo));
        assertNotNull(AnnotationUtil.getVClock(pojo, new BasicVClock(clockBytes) ));
    }
    
    @Test
    public void nullVClockMethod()
    {
        final VClock expected = null;
        final PojoWithAnnotatedMethods pojo = new PojoWithAnnotatedMethods();
        pojo.setVClock(expected);

        assertNull(AnnotationUtil.getVClock(pojo));
        final byte[] clockBytes = "a85hYGBgzGDKBVIcypz/fvo/2e2UwZTImMfKwHIy/SRfFgA=".getBytes();
        assertNotNull(AnnotationUtil.getVClock(pojo, new BasicVClock(clockBytes)));
    }
   
    
    @Test
    public void getTombstoneField()
    {
        final PojoWithAnnotatedFields pojo = new PojoWithAnnotatedFields();
        pojo.tombstone = true;
        assertTrue(AnnotationUtil.getTombstone(pojo));
    }
    
    @Test
    public void getTombstoneMethod()
    {
        final PojoWithAnnotatedMethods pojo = new PojoWithAnnotatedMethods();
        pojo.setTombstone(true);
        assertTrue(AnnotationUtil.getTombstone(pojo));
    }
    
    @Test
    public void noTombstoneFieldOrMethod()
    {
        final Object o = new Object();

        assertNull(AnnotationUtil.getTombstone(o));
    }
    
    @Test
    public void illegalTombstoneFieldType()
    {
        final Object o = new Object()
        {
            @RiakTombstone
            private final String domainProperty = null;

        };

        try
        {
            boolean tombstone = AnnotationUtil.getTombstone(o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
    }
    
    @Test
    public void illegalTombstoneGetterType()
    {
        final Object o = new Object()
        {
            @RiakTombstone
            public String getTombstone()
            {
                return null;
            }
            
            @RiakVClock
            public void setTombstone(Boolean v)
            {
            }

        };
        
        try
        {
            boolean tombstone = AnnotationUtil.getTombstone(o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
    }
    
    @Test
    public void illegalTombstoneSetterType()
    {
        final Object o = new Object()
        {
            @RiakTombstone
            public Boolean getTombstone()
            {
                return null;
            }
            
            @RiakVClock
            public void setTombstone(String v)
            {}

        };
        
        try
        {
            boolean tombstone = AnnotationUtil.getTombstone(o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
    }
    
    @Test
    public void setTombstoneField()
    {
        final PojoWithAnnotatedFields pojo = new PojoWithAnnotatedFields();
        
        AnnotationUtil.setTombstone(pojo, true);
        assertEquals(pojo.tombstone, true);
        
        final PojoWithAnnotatedByteFields pojo2 = new PojoWithAnnotatedByteFields();
        
        AnnotationUtil.setTombstone(pojo2, true);
        assertEquals(pojo2.tombstone, true);
    }
    
    @Test
    public void setTombstoneMethod()
    {
        final PojoWithAnnotatedMethods pojo = new PojoWithAnnotatedMethods();
        
        AnnotationUtil.setTombstone(pojo, true);
        assertTrue(pojo.getTombstone());
        
        final PojoWithAnnotatedByteMethods pojo2 = new PojoWithAnnotatedByteMethods();
        
        AnnotationUtil.setTombstone(pojo2, true);
        assertTrue(pojo2.getTombstone());
        
    }
    
    @Test
    public void populateIndexFields()
    {
        HashSet<String> langs = new HashSet<String>(Arrays.asList("c","erlang","java"));
        HashSet<Long> longs = new HashSet<Long>(Arrays.asList(4L,9L,12L));
        HashSet<BinaryValue> bytes = new HashSet<BinaryValue>();
        
        for (Integer i = 0; i < 5; i++)
        {
            bytes.add(BinaryValue.create(i.toString().getBytes()));
        }
        
        RiakIndexes rIndexes = new RiakIndexes();
        
        rIndexes.getIndex(StringBinIndex.named("favorite_languages")).add(langs);
        rIndexes.getIndex(StringBinIndex.named("lucky_language")).add(langs);
        rIndexes.getIndex(LongIntIndex.named("longs")).add(longs);
        rIndexes.getIndex(LongIntIndex.named("lucky_long")).add(longs);
        rIndexes.getIndex(LongIntIndex.named("lucky_longlong")).add(longs);
        rIndexes.getIndex(RawIndex.named("raw", IndexType.INT)).add(bytes);
        rIndexes.getIndex(RawIndex.named("set_raw", IndexType.INT)).add(bytes);
        rIndexes.getIndex(RawIndex.named("raw", IndexType.BIN)).add(bytes);
        rIndexes.getIndex(RawIndex.named("set_raw", IndexType.BIN)).add(bytes);
        
        PojoWithAnnotatedFields pojo = new PojoWithAnnotatedFields();
        
        AnnotationUtil.populateIndexes(rIndexes, pojo);
        
        assertNotNull("Expected bin index field (Set<String> languages) to be populated", pojo.languages);
        assertNotNull("Expected bin index field (String luckyLanguage) to be populated", pojo.luckyLanguage);
        assertNotNull("Expected int index field (Set<Long> longs) to be populated", pojo.longs);
        assertTrue("Expected int index field (long luckyLong) to be populated", pojo.luckyLong != 0);
        assertNotNull("Expected int index field (Long luckyLongLong) to be populated", pojo.luckyLongLong);
        assertNotNull("Expected int index field (byte[] rawInt) to be populated", pojo.rawInt);
        assertNotNull("Expected bin index field (byte[] rawBin) to be populated", pojo.rawBin);
        assertNotNull("Expected int index field (Set<byte[]> rawInts) to be populated", pojo.rawInts);
        assertNotNull("Expected bin index field (Set<byte[]> rawBins) to be populated", pojo.rawBins);
        
        assertEquals(langs.size(), pojo.languages.size());
        assertEquals(langs.iterator().next(), pojo.luckyLanguage);
        assertEquals(longs.size(), pojo.longs.size());
        assertEquals(longs.iterator().next().longValue(), pojo.luckyLong);
        assertEquals(longs.iterator().next(), pojo.luckyLongLong);
        assertEquals(bytes.size(), pojo.rawInts.size());
        assertEquals(bytes.size(), pojo.rawBins.size());
    }
    
    @Test
    public void getIndexFields()
    {
        PojoWithAnnotatedFields pojo = new PojoWithAnnotatedFields();
        pojo.languages = new HashSet<String>(Arrays.asList("c","erlang","java"));
        pojo.longs = new HashSet<Long>(Arrays.asList(4L,9L,12L));
        pojo.luckyLanguage = pojo.languages.iterator().next();
        pojo.luckyLong = pojo.longs.iterator().next();
        pojo.luckyLongLong = pojo.longs.iterator().next();
        
        pojo.rawBins = new HashSet<byte[]>();
        pojo.rawInts = new HashSet<byte[]>();
        
        for (Integer i = 0; i < 5; i++)
        {
            byte[] bytes = i.toString().getBytes();
            pojo.rawBins.add(bytes);
            pojo.rawInts.add(bytes);
        }
        
        pojo.rawInt = pojo.rawInts.iterator().next();
        pojo.rawBin = pojo.rawBins.iterator().next();
        
        RiakIndexes rIndexes = new RiakIndexes();
        AnnotationUtil.getIndexes(rIndexes, pojo);
        
        assertEquals("Expected RiakIndexes BinIndex (favorite_languages) to be populated", 
                      rIndexes.getIndex(StringBinIndex.named("favorite_languages")).size(), 
                      pojo.languages.size());        
        assertEquals("Expected RiakIndexes BinIndex (lucky_language) to be populated", 
                      rIndexes.getIndex(StringBinIndex.named("lucky_language")).size(), 
                      1);
        assertEquals("Expected RiakIndexes BinIndex (set_raw) to be pupulated",
                      rIndexes.getIndex(RawIndex.named("set_raw", IndexType.BIN)).size(),
                      pojo.rawBins.size());
        assertEquals("Expected RiakIndexes BinIndex (raw) to be pupulated",
                      rIndexes.getIndex(RawIndex.named("raw", IndexType.BIN)).size(),
                      1);
        
        
        assertEquals("Expected RiakIndexes IntIndex (longs) to be populated", 
                      rIndexes.getIndex(LongIntIndex.named("longs")).size(), 
                      pojo.longs.size());
        assertEquals("Expected RiakIndexes IntIndex (lucky_long) to be populated", 
                      rIndexes.getIndex(LongIntIndex.named("lucky_long")).size(), 
                      1);
        assertEquals("Expected RiakIndexes IntIndex (lucky_longlong) to be populated", 
                      rIndexes.getIndex(LongIntIndex.named("lucky_longlong")).size(), 
                      1);
        assertEquals("Expected RiakIndexes IntIndex (set_raw) to be pupulated",
                      rIndexes.getIndex(RawIndex.named("set_raw", IndexType.INT)).size(),
                      pojo.rawInts.size());
        assertEquals("Expected RiakIndexes IntIndex (raw) to be pupulated",
                      rIndexes.getIndex(RawIndex.named("raw", IndexType.INT)).size(),
                      1);
        
        
        assertTrue(rIndexes.getIndex(StringBinIndex.named("favorite_languages")).values().containsAll(pojo.languages));
        assertEquals(rIndexes.getIndex(StringBinIndex.named("lucky_language")).iterator().next(), 
                     pojo.languages.iterator().next());
        
        assertTrue(rIndexes.getIndex(LongIntIndex.named("longs")).values().containsAll(pojo.longs));
        assertEquals(rIndexes.getIndex(LongIntIndex.named("lucky_long")).iterator().next(), 
                     pojo.longs.iterator().next());
        assertEquals(rIndexes.getIndex(LongIntIndex.named("lucky_longlong")).iterator().next(), 
                     pojo.longs.iterator().next());
        assertArrayEquals(rIndexes.getIndex(RawIndex.named("raw", IndexType.INT)).iterator().next().getValue(),
                     pojo.rawInt);
        
    }
    
    @Test
    public void populateIndexMethods()
    {
        HashSet<String> strings = new HashSet<String>(Arrays.asList("c","erlang","java"));
        HashSet<Long> longs = new HashSet<Long>(Arrays.asList(4L,9L,12L));
        HashSet<BinaryValue> bytes = new HashSet<BinaryValue>();
        
        for (Integer i = 0; i < 5; i++)
        {
            bytes.add(BinaryValue.create(i.toString().getBytes()));
        }
        
        RiakIndexes rIndexes = new RiakIndexes();
        
        rIndexes.getIndex(StringBinIndex.named("strings")).add(strings);
        rIndexes.getIndex(StringBinIndex.named("string")).add(strings);
        rIndexes.getIndex(LongIntIndex.named("longs")).add(longs);
        rIndexes.getIndex(LongIntIndex.named("long")).add(longs);
        rIndexes.getIndex(LongIntIndex.named("longlong")).add(longs);
        rIndexes.getIndex(RawIndex.named("raw", IndexType.INT)).add(bytes);
        rIndexes.getIndex(RawIndex.named("set_raw", IndexType.INT)).add(bytes);
        rIndexes.getIndex(RawIndex.named("raw", IndexType.BIN)).add(bytes);
        rIndexes.getIndex(RawIndex.named("set_raw", IndexType.BIN)).add(bytes);
        
        PojoWithAnnotatedMethods pojo = new PojoWithAnnotatedMethods();
        
        AnnotationUtil.populateIndexes(rIndexes, pojo);
        
        assertNotNull("Expected bin index field (Set<String> strings) to be populated", pojo.getStrings());
        assertNotNull("Expected bin index field (String string) to be populated", pojo.getString());
        assertNotNull("Expected int index field (Set<Long> longs) to be populated", pojo.getLongs());
        assertTrue("Expected int index field (long long) to be populated", pojo.getLong() != 0);
        assertNotNull("Expected int index field (Long longlong) to be populated", pojo.getLongLong());
        
        assertNotNull("Expected bin index field (Set<byte> rawBins) to be populated", pojo.getRawBinsIndex());
        assertNotNull("Expected int index field (Set<byte> rawInts) to be populated", pojo.getRawIntsIndex());
        assertNotNull("Expected bin index field (byte[] rawBin) to be populated", pojo.getRawBinIndex());
        assertNotNull("Expected int index field (byte[] rawInt) to be populated", pojo.getRawIntIndex());
        
        assertEquals(strings.size(), pojo.getStrings().size());
        assertEquals(strings.iterator().next(), pojo.getString());
        assertEquals(longs.size(), pojo.getLongs().size());
        assertEquals(longs.iterator().next().longValue(), pojo.getLong());
        assertEquals(longs.iterator().next(), pojo.getLongLong());
        assertEquals(bytes.size(), pojo.getRawBinsIndex().size() );
        assertEquals(bytes.size(), pojo.getRawIntsIndex().size() );
    }

    @Test
    public void getIndexMethods()
    {
        PojoWithAnnotatedMethods pojo = new PojoWithAnnotatedMethods();
        pojo.setStrings(new HashSet<String>(Arrays.asList("c","erlang","java")));
        pojo.setLongs(new HashSet<Long>(Arrays.asList(4L,9L,12L)));
        pojo.setString(pojo.getStrings().iterator().next());
        pojo.setLong(pojo.getLongs().iterator().next());
        pojo.setLongLong(pojo.longs.iterator().next());
        
        pojo.rawBins = new HashSet<byte[]>();
        pojo.rawInts = new HashSet<byte[]>();
        
        for (Integer i = 0; i < 5; i++)
        {
            byte[] bytes = i.toString().getBytes();
            pojo.rawBins.add(bytes);
            pojo.rawInts.add(bytes);
        }
        
        pojo.rawInt = pojo.rawInts.iterator().next();
        pojo.rawBin = pojo.rawBins.iterator().next();
        
        RiakIndexes rIndexes = new RiakIndexes();
        AnnotationUtil.getIndexes(rIndexes, pojo);
        
        assertEquals("Expected RiakIndexes BinIndex (strings) to be populated", 
                      rIndexes.getIndex(StringBinIndex.named("strings")).size(), 
                      pojo.getStrings().size());        
        assertEquals("Expected RiakIndexes BinIndex (string) to be populated", 
                      rIndexes.getIndex(StringBinIndex.named("string")).size(), 
                      1);
        assertEquals("Expected RiakIndexes BinIndex (set_raw) to be pupulated",
                      rIndexes.getIndex(RawIndex.named("set_raw", IndexType.BIN)).size(),
                      pojo.rawBins.size());
        assertEquals("Expected RiakIndexes BinIndex (raw) to be pupulated",
                      rIndexes.getIndex(RawIndex.named("raw", IndexType.BIN)).size(),
                      1);
        
        assertEquals("Expected RiakIndexes IntIndex (longs) to be populated", 
                      rIndexes.getIndex(LongIntIndex.named("longs")).size(), 
                      pojo.longs.size());
        assertEquals("Expected RiakIndexes IntIndex (long) to be populated", 
                      rIndexes.getIndex(LongIntIndex.named("long")).size(), 
                      1);
        assertEquals("Expected RiakIndexes IntIndex (longlong) to be populated", 
                      rIndexes.getIndex(LongIntIndex.named("longlong")).size(), 
                      1);
        assertEquals("Expected RiakIndexes IntIndex (set_raw) to be pupulated",
                      rIndexes.getIndex(RawIndex.named("set_raw", IndexType.INT)).size(),
                      pojo.rawInts.size());
        assertEquals("Expected RiakIndexes IntIndex (raw) to be pupulated",
                      rIndexes.getIndex(RawIndex.named("raw", IndexType.INT)).size(),
                      1);
        
        assertTrue(rIndexes.getIndex(StringBinIndex.named("strings")).values().containsAll(pojo.getStrings()));
        assertEquals(rIndexes.getIndex(StringBinIndex.named("string")).iterator().next(), 
                     pojo.getStrings().iterator().next());
        
        assertTrue(rIndexes.getIndex(LongIntIndex.named("longs")).values().containsAll(pojo.getLongs()));
        assertEquals(rIndexes.getIndex(LongIntIndex.named("long")).iterator().next(), 
                     pojo.getLongs().iterator().next());
        assertEquals(rIndexes.getIndex(LongIntIndex.named("longlong")).iterator().next(), 
                     pojo.getLongs().iterator().next());
        
    }
    
    @Test
    public void illegalRiakIndexFieldType()
    {
        final RiakIndexes rIndexes = new RiakIndexes();
        
        Object o = new Object()
        {
            @RiakIndex(name="whatever")
            private final Boolean domainProperty = null;

        };
        
        try
        {
            AnnotationUtil.populateIndexes(rIndexes, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
        o = new Object()
        {
            @RiakIndex(name="whatever")
            private final Set<Boolean> domainProperty = null;

        };
        
        try
        {
            AnnotationUtil.populateIndexes(rIndexes, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
        
    }
    
    @Test
    public void illegalRiakIndexSetterType()
    {
        final RiakIndexes rIndexes = new RiakIndexes();
        
        Object o = new Object()
        {
            @RiakIndex(name="whatever")
            public void setIndex(Boolean b)
            {}
            
            @RiakIndex(name="whatever")
            public Set<Long> getIndex() 
            {
                return null;
            }

        };
        
        try
        {
            AnnotationUtil.populateIndexes(rIndexes, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
        o = new Object()
        {
            @RiakIndex(name="whatever")
            public void setIndex(Set<Boolean> index)
            {}
            
            @RiakIndex(name="whatever")
            public Set<Long> getIndex() 
            {
                return null;
            }

        };
        
        try
        {
            AnnotationUtil.populateIndexes(rIndexes, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
    }
    
    @Test
    public void illegalRiakIndexGetterType()
    {
        final RiakIndexes rIndexes = new RiakIndexes();
        
        Object o = new Object()
        {
            @RiakIndex(name="whatever")
            public void setIndex(Long b)
            {}
            
            @RiakIndex(name="whatever")
            public boolean getIndex() 
            {
                return true;
            }

        };
        
        try
        {
            AnnotationUtil.populateIndexes(rIndexes, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
        o = new Object()
        {
            @RiakIndex(name="whatever")
            public void setIndex(Set<Long> index)
            {}
            
            @RiakIndex(name="whatever")
            public Set<Integer> getIndex() 
            {
                return null;
            }

        };
        
        try
        {
            AnnotationUtil.populateIndexes(rIndexes, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
    }
    
    
    @Test
    public void missingIndexNameInAnnotation()
    {
        final RiakIndexes rIndexes = new RiakIndexes();
        
        Object o = new Object()
        {
            @RiakIndex(name = "")
            public void setIndex(Set<String> index)
            {}
            
            @RiakIndex(name = "")
            public Set<String> getIndex() 
            {
                return null;
            }

        };
        
        try
        {
            AnnotationUtil.populateIndexes(rIndexes, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
        o = new Object()
        {
            @RiakIndex(name = "")
            private String index = null;

        };
        
        try
        {
            AnnotationUtil.populateIndexes(rIndexes, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
    }
    
    @Test
    public void rawIndexWithoutFullName()
    {
        final RiakIndexes rIndexes = new RiakIndexes();
        
        Object o = new Object()
        {
            @RiakIndex(name="raw")
            public void setIndex(Set<byte[]> index)
            {}
            
            @RiakIndex(name="raw_int")
            public Set<byte[]> getIndex() 
            {
                return null;
            }

        };
        
        try
        {
            AnnotationUtil.populateIndexes(rIndexes, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
        o = new Object()
        {
            @RiakIndex(name="raw")
            private byte[] index = null;

        };
        
        try
        {
            AnnotationUtil.populateIndexes(rIndexes, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
    }
    
    @Test
    public void annotatedFieldNotInRiak()
    {
        PojoWithAnnotatedFields pojo = new PojoWithAnnotatedFields();
        RiakIndexes rIndex = new RiakIndexes();
        
        AnnotationUtil.populateIndexes(rIndex, pojo);
        
        assertNotNull(pojo.languages);
        assertTrue(pojo.languages.isEmpty());
        assertNotNull(pojo.longs);
        assertTrue(pojo.longs.isEmpty());
        
        // Long/String will be null, long will be zero
        assertNull(pojo.luckyLongLong);
        assertNull(pojo.luckyLanguage);
        assertEquals(pojo.luckyLong, 0);
        
    }
    
    @Test 
    public void annotatedMethodNotInRiak()
    {
        PojoWithAnnotatedMethods pojo = new PojoWithAnnotatedMethods();
        RiakIndexes rIndex = new RiakIndexes();
        
        AnnotationUtil.populateIndexes(rIndex, pojo);
        
        assertNotNull(pojo.getStrings());
        assertTrue(pojo.getStrings().isEmpty());
        assertNotNull(pojo.getLongs());
        assertTrue(pojo.getLongs().isEmpty());
        
        // Long/String will be null, long will be zero
        assertNull(pojo.getLongLong());
        assertNull(pojo.getString());
        assertEquals(pojo.getLong(), 0);
        
    }
    
    @Test
    public void noAnnotatedFieldOrMethodForIndex()
    {
        PojoWithAnnotatedFields pojo = new PojoWithAnnotatedFields();
        RiakIndexes rIndex = new RiakIndexes();
        rIndex.getIndex(StringBinIndex.named("no_annotation")).add("value");
        
        AnnotationUtil.populateIndexes(rIndex, pojo); // should do nothing
        
        rIndex = new RiakIndexes();
        AnnotationUtil.getIndexes(rIndex, pojo);
        assertFalse(rIndex.hasIndex(StringBinIndex.named("no_annotation")));
    }
    
    @Test
    public void annotatedIndexFieldIsNull()
    {
        PojoWithAnnotatedFields pojo = new PojoWithAnnotatedFields();
        RiakIndexes rIndex = new RiakIndexes();
        AnnotationUtil.getIndexes(rIndex, pojo);
        
        // The indexes should be created, but be empty
        assertFalse(rIndex.isEmpty());
        assertEquals(9, rIndex.size());
        
        assertTrue(rIndex.hasIndex(StringBinIndex.named("favorite_languages")));
        assertTrue(rIndex.getIndex(StringBinIndex.named("favorite_languages")).isEmpty());
        assertTrue(rIndex.hasIndex(LongIntIndex.named("longs")));
        assertTrue(rIndex.getIndex(LongIntIndex.named("longs")).isEmpty());
        assertTrue(rIndex.hasIndex(StringBinIndex.named("lucky_language")));
        assertTrue(rIndex.getIndex(StringBinIndex.named("lucky_language")).isEmpty());
        assertTrue(rIndex.hasIndex(LongIntIndex.named("lucky_longlong")));
        assertTrue(rIndex.getIndex(LongIntIndex.named("lucky_longlong")).isEmpty());
        
        // TODO: chuck long support
        assertTrue(rIndex.hasIndex(LongIntIndex.named("lucky_long")));
        assertEquals(1, rIndex.getIndex(LongIntIndex.named("lucky_long")).size());
        
    }
    
    @Test
    public void annotatedIndexMethodIsNull()
    {
        PojoWithAnnotatedMethods pojo = new PojoWithAnnotatedMethods();
        RiakIndexes rIndex = new RiakIndexes();
        AnnotationUtil.getIndexes(rIndex, pojo);
        
        // The indexes should be created, but be empty
        assertFalse(rIndex.isEmpty());
        assertEquals(9, rIndex.size());

        assertTrue(rIndex.hasIndex(StringBinIndex.named("strings")));
        assertTrue(rIndex.getIndex(StringBinIndex.named("strings")).isEmpty());
        
        assertTrue(rIndex.hasIndex(LongIntIndex.named("longs")));
        assertTrue(rIndex.getIndex(LongIntIndex.named("longs")).isEmpty());
        assertTrue(rIndex.hasIndex(StringBinIndex.named("string")));
        assertTrue(rIndex.getIndex(StringBinIndex.named("string")).isEmpty());
        assertTrue(rIndex.hasIndex(LongIntIndex.named("longlong")));
        assertTrue(rIndex.getIndex(LongIntIndex.named("longlong")).isEmpty());
        
        // TODO: chuck long support
        assertTrue(rIndex.hasIndex(LongIntIndex.named("long")));
        assertEquals(1, rIndex.getIndex(LongIntIndex.named("long")).size());
        
    }
    
    @Test
    public void getRiakLinksField()
    {
        Collection<RiakLink> links = new LinkedList<RiakLink>();
        RiakLink link = new RiakLink("bucket", "key", "tag");
        links.add(link);
        link = new RiakLink("bucket", "key2", "tag");
        links.add(link);
        PojoWithAnnotatedFields pojo = new PojoWithAnnotatedFields();
        pojo.links = links;
        
        com.basho.riak.client.core.query.links.RiakLinks riakLinks =
            new com.basho.riak.client.core.query.links.RiakLinks();
        
        AnnotationUtil.getLinks(riakLinks, pojo);
        assertFalse(riakLinks.isEmpty());
        assertTrue(riakLinks.getLinks().containsAll(links));
    }
    
    @Test
    public void getRiakLinksMethod()
    {
        Collection<RiakLink> links = new LinkedList<RiakLink>();
        RiakLink link = new RiakLink("bucket", "key", "tag");
        links.add(link);
        link = new RiakLink("bucket", "key2", "tag");
        links.add(link);
        PojoWithAnnotatedMethods pojo = new PojoWithAnnotatedMethods();
        pojo.setLinks(links);
        
        com.basho.riak.client.core.query.links.RiakLinks riakLinks =
            new com.basho.riak.client.core.query.links.RiakLinks();
        
        AnnotationUtil.getLinks(riakLinks, pojo);
        assertFalse(riakLinks.isEmpty());
        assertTrue(riakLinks.getLinks().containsAll(links));
    }
    
    @Test
    public void setRiakLinksField()
    {
        com.basho.riak.client.core.query.links.RiakLinks riakLinks =
            new com.basho.riak.client.core.query.links.RiakLinks();
        RiakLink link = new RiakLink("bucket", "key", "tag");
        riakLinks.addLink(link);
        link = new RiakLink("bucket", "key2", "tag");
        riakLinks.addLink(link);
        
        PojoWithAnnotatedFields pojo = new PojoWithAnnotatedFields();
        AnnotationUtil.populateLinks(riakLinks, pojo);
        
        assertNotNull(pojo.links);
        assertEquals(riakLinks.size(), pojo.links.size());
        assertTrue(pojo.links.containsAll(riakLinks.getLinks()));
    }
    
    @Test
    public void setRiakLinksMethod()
    {
        com.basho.riak.client.core.query.links.RiakLinks riakLinks =
            new com.basho.riak.client.core.query.links.RiakLinks();
        RiakLink link = new RiakLink("bucket", "key", "tag");
        riakLinks.addLink(link);
        link = new RiakLink("bucket", "key2", "tag");
        riakLinks.addLink(link);
        
        PojoWithAnnotatedMethods pojo = new PojoWithAnnotatedMethods();
        AnnotationUtil.populateLinks(riakLinks, pojo);
        
        assertNotNull(pojo.getLinks());
        assertEquals(riakLinks.size(), pojo.getLinks().size());
        assertTrue(pojo.getLinks().containsAll(riakLinks.getLinks()));
    }

    @Test
    public void illegalRiakLinksFieldType()
    {
        com.basho.riak.client.core.query.links.RiakLinks riakLinks =
            new com.basho.riak.client.core.query.links.RiakLinks();
        Object o = new Object()
        {
            @RiakLinks
            private final String domainProperty = null;

        };
        
        try
        {
             AnnotationUtil.getLinks(riakLinks, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
        o = new Object()
        {
            @RiakLinks
            private final Collection<String> domainProperty = null;

        };
        
        try
        {
            AnnotationUtil.getLinks(riakLinks, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
    }
    
    @Test 
    public void illegalRiakLinksGetterType()
    {
        com.basho.riak.client.core.query.links.RiakLinks riakLinks =
            new com.basho.riak.client.core.query.links.RiakLinks();
        
        Object o = new Object()
        {
            @RiakLinks
            public String getLinks()
            {
                return null;
            }
            
            @RiakLinks
            public void setLinks(Collection<RiakLink> links) {}
            
        };
        
        try
        {
            AnnotationUtil.getLinks(riakLinks, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
        o = new Object()
        {
            @RiakLinks
            public Collection<String> getLinks()
            {
                return null;
            }
            
            @RiakLinks
            public void setLinks(Collection<RiakLink> links) {}
            
        };
        
        try
        {
            AnnotationUtil.getLinks(riakLinks, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
    }
    
    @Test 
    public void illegalRiakLinksSetterType()
    {
        com.basho.riak.client.core.query.links.RiakLinks riakLinks =
            new com.basho.riak.client.core.query.links.RiakLinks();
        
        Object o = new Object()
        {
            @RiakLinks
            public Collection<RiakLink> getLinks()
            {
                return null;
            }
            
            @RiakLinks
            public void setLinks(String links) {}
            
        };
        
        try
        {
            AnnotationUtil.getLinks(riakLinks, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
        o = new Object()
        {
            @RiakLinks
            public Collection<RiakLink> getLinks()
            {
                return null;
            }
            
            @RiakLinks
            public void setLinks(Collection<String> links) {}
            
        };
        
        try
        {
            AnnotationUtil.getLinks(riakLinks, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        } 
    }
    
    @Test
    public void annotatedLinksFieldIsNull()
    {
        PojoWithAnnotatedFields pojo = new PojoWithAnnotatedFields();
        com.basho.riak.client.core.query.links.RiakLinks riakLinks =
            new com.basho.riak.client.core.query.links.RiakLinks();
        
        AnnotationUtil.getLinks(riakLinks, pojo);
        
        assertTrue(riakLinks.isEmpty());
    }
    
    @Test
    public void annotatedLinksMethodIsNull()
    {
        PojoWithAnnotatedMethods pojo = new PojoWithAnnotatedMethods();
        com.basho.riak.client.core.query.links.RiakLinks riakLinks =
            new com.basho.riak.client.core.query.links.RiakLinks();
        
        AnnotationUtil.getLinks(riakLinks, pojo);
        
        assertTrue(riakLinks.isEmpty());
    }
    
    @Test
    public void noAnnotatedLinksFieldOrMethod()
    {
        com.basho.riak.client.core.query.links.RiakLinks riakLinks =
            new com.basho.riak.client.core.query.links.RiakLinks();
        Object o = new Object();
        
        RiakLink link = new RiakLink("bucket", "key", "tag");
        riakLinks.addLink(link);
        link = new RiakLink("bucket", "key2", "tag");
        riakLinks.addLink(link);
        AnnotationUtil.populateLinks(riakLinks, o); // should do nothing
        
        riakLinks.removeAllLinks();
        AnnotationUtil.getLinks(riakLinks, o);
        
        assertTrue(riakLinks.isEmpty());
    }
    
    @Test public void getUsermetaField() 
    {
        final String userMetaItemOne = "userMetaItemOne";
        final Map<String, String> userMetaData = new HashMap<String, String>() {{
                put("key1", "value1");
                put("key2", "value2");
                put("key3", "value3");
        }};
           
        PojoWithAnnotatedFields obj = new PojoWithAnnotatedFields();
        obj.usermeta = userMetaData;
        obj.metaItemOne = userMetaItemOne;
        
        RiakUserMetadata meta = new RiakUserMetadata();
        AnnotationUtil.getUsermetaData(meta, obj);
                
        for (String key : userMetaData.keySet())
        {
            assertTrue(meta.containsKey(key));
            assertEquals(meta.get(key), userMetaData.get(key));
        }
        
        assertTrue(meta.containsKey(META_KEY_ONE));
        assertEquals(userMetaItemOne, meta.get(META_KEY_ONE));
    }

    @Test
    public void getUserMetaMethod()
    {
        final String userMetaItemOne = "userMetaItemOne";
        final Map<String, String> userMetaData = new HashMap<String, String>() {{
                put("key1", "value1");
                put("key2", "value2");
                put("key3", "value3");
        }};
           
        PojoWithAnnotatedMethods obj = new PojoWithAnnotatedMethods();
        obj.setUsermeta(userMetaData);
        obj.setMetaItemOne(userMetaItemOne); 
        
        RiakUserMetadata meta = new RiakUserMetadata();
        AnnotationUtil.getUsermetaData(meta, obj);        
                
        for (String key : userMetaData.keySet())
        {
            assertTrue(meta.containsKey(key));
            assertEquals(meta.get(key), userMetaData.get(key));
        }
        
        assertTrue(meta.containsKey(META_KEY_ONE));
        assertEquals(userMetaItemOne, meta.get(META_KEY_ONE));
    }
    
    @Test
    public void setUserMetaField()
    {
        RiakUserMetadata meta = new RiakUserMetadata();
        meta.put("key1", "value1");
        meta.put("key2", "value2");
        meta.put("key3", "value3");
        meta.put(META_KEY_ONE, "userMetaItemOne");
        
        PojoWithAnnotatedFields obj = new PojoWithAnnotatedFields();
        AnnotationUtil.populateUsermeta(meta, obj);
        
        assertNotNull(obj.usermeta);
        // The single annotated field is removed
        assertEquals(3, obj.usermeta.size());
        
        for (String key : obj.usermeta.keySet())
        {
            assertTrue(meta.containsKey(key));
            assertEquals(meta.get(key), obj.usermeta.get(key));
        }
        
        assertNotNull(obj.metaItemOne);
        assertEquals("userMetaItemOne", obj.metaItemOne);
        
    }
    
    @Test
    public void setUserMetaMethod()
    {
        RiakUserMetadata meta = new RiakUserMetadata();
        meta.put("key1", "value1");
        meta.put("key2", "value2");
        meta.put("key3", "value3");
        meta.put(META_KEY_ONE, "userMetaItemOne");
        
        PojoWithAnnotatedMethods obj = new PojoWithAnnotatedMethods();
        AnnotationUtil.populateUsermeta(meta, obj);
        
        assertNotNull(obj.usermeta);
        // The single annotated field is removed
        assertEquals(3, obj.usermeta.size());
        
        for (String key : obj.usermeta.keySet())
        {
            assertTrue(meta.containsKey(key));
            assertEquals(meta.get(key), obj.usermeta.get(key));
        }
        
        assertNotNull(obj.metaItemOne);
        assertEquals("userMetaItemOne", obj.metaItemOne);
        
    }
    
    @Test
    public void illegalUserMetaFieldType()
    {
        RiakUserMetadata meta = new RiakUserMetadata();
        
        Object o = new Object() {
            @RiakUsermeta
            Boolean meta;
        };
        
        try
        {
            AnnotationUtil.populateUsermeta(meta, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
        o = new Object() {
            @RiakUsermeta
            Map<Long, Long> meta;
        };
        
        try
        {
            AnnotationUtil.populateUsermeta(meta, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
        o = new Object() {
            @RiakUsermeta(key="metaKey")
            Long meta;
        };
        
        try
        {
            AnnotationUtil.populateUsermeta(meta, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
    }
    
    @Test
    public void illegalUserMetaGetterType()
    {
        RiakUserMetadata meta = new RiakUserMetadata();
        
        Object o = new Object() {
            @RiakUsermeta
            public Boolean getMeta()
            {
                return true;
            };
            
            @RiakUsermeta
            public void setMeta(Map<String,String> meta) {}
        };
        
        try
        {
            AnnotationUtil.populateUsermeta(meta, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
        o = new Object() {
            @RiakUsermeta
            public Map<Long, Long> getMeta()
            {
                return null;
            };
            
            @RiakUsermeta
            public void setMeta(Map<String,String> meta) {}
        };
        
        try
        {
            AnnotationUtil.populateUsermeta(meta, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
        o = new Object() {
            @RiakUsermeta(key="metaKey")
            public Long getMeta()
            {
                return 0L;
            };
            @RiakUsermeta(key="metaKey")
            public void setMeta(String meta) {}
        };
        
        try
        {
            AnnotationUtil.populateUsermeta(meta, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
    }
    
    @Test
    public void illegalUserMetaSetterType()
    {
        RiakUserMetadata meta = new RiakUserMetadata();
        
        Object o = new Object() {
            @RiakUsermeta
            public Map<String,String> getMeta()
            {
                return null;
            };
            
            @RiakUsermeta
            public void setMeta(Boolean meta) {}
        };
        
        try
        {
            AnnotationUtil.populateUsermeta(meta, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
        
        o = new Object() {
            @RiakUsermeta
            public Map<String, String> getMeta()
            {
                return null;
            };
            
            @RiakUsermeta
            public void setMeta(Map<Long,Long> meta) {}
        };
        
        try
        {
            AnnotationUtil.populateUsermeta(meta, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
        o = new Object() {
            @RiakUsermeta(key="metaKey")
            public String getMeta()
            {
                return null;
            };
            @RiakUsermeta(key="metaKey")
            public void setMeta(Long meta) {}
        };
        
        try
        {
            AnnotationUtil.populateUsermeta(meta, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
    }
    
    
    @Test
    public void singleMetaAnnotationMissingKey()
    {
        RiakUserMetadata meta = new RiakUserMetadata();
        
        Object o = new Object() {
            @RiakUsermeta
            String meta;
        };
        
        try
        {
            AnnotationUtil.populateUsermeta(meta, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
        o = new Object() {
            @RiakUsermeta
            public String getSingleMeta()
            {
                return "";
            }
            
            @RiakUsermeta(key="key")
            public void setSingleMeta(String value) {}
        };
        
        try
        {
            AnnotationUtil.populateUsermeta(meta, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
    }
    
    @Test
    public void annotatedMetaFieldIsNull()
    {
        PojoWithAnnotatedFields pojo = new PojoWithAnnotatedFields();
        RiakUserMetadata meta = new RiakUserMetadata();
        AnnotationUtil.getUsermetaData(meta, pojo);
        
        assertTrue(meta.isEmpty());
        
    }
    
    @Test
    public void annotatedMetaMethodIsNull()
    {
        PojoWithAnnotatedMethods pojo = new PojoWithAnnotatedMethods();
        RiakUserMetadata meta = new RiakUserMetadata();
        AnnotationUtil.getUsermetaData(meta, pojo);
        
        assertTrue(meta.isEmpty());
    }
    
    @Test
    public void riakMetaEmptyFields()
    {
        PojoWithAnnotatedFields pojo = new PojoWithAnnotatedFields();
        RiakUserMetadata meta = new RiakUserMetadata();
        AnnotationUtil.populateUsermeta(meta, pojo);
        
        assertNotNull(pojo.usermeta);
        // Single entries should be null
        assertNull(pojo.metaItemOne);
    }
    
    @Test
    public void riakMetaEmptyMethods()
    {
        PojoWithAnnotatedMethods pojo = new PojoWithAnnotatedMethods();
        RiakUserMetadata meta = new RiakUserMetadata();
        AnnotationUtil.populateUsermeta(meta, pojo);
        
        assertNotNull(pojo.getUsermeta());
        // Single entries should be null
        assertNull(pojo.getMetaItemOne());
    }
    
    @Test
    public void getContentTypeField()
    {
        final String expected = "application/octet-stream";
        final PojoWithAnnotatedFields pojo = new PojoWithAnnotatedFields();
        pojo.contentType = expected;
        
        assertEquals(expected, AnnotationUtil.getContentType(pojo));
        assertEquals(expected, AnnotationUtil.getContentType(pojo, null));
    }

    @Test
    public void setContentTypeField()
    {
        final PojoWithAnnotatedFields pojo = new PojoWithAnnotatedFields();
        final String expected = "application/octet-stream";
        AnnotationUtil.setContentType(pojo, expected);
        
        assertNotNull(pojo.contentType);
        assertEquals(expected.toString(), pojo.contentType);
    }
    
    @Test
    public void getContentTypeMethod()
    {
        final String expected = "application/octet-stream";
        final PojoWithAnnotatedMethods pojo = new PojoWithAnnotatedMethods();
        pojo.setContentType(expected);
        
        assertEquals(expected, AnnotationUtil.getContentType(pojo));
        assertEquals(expected, AnnotationUtil.getContentType(pojo, null));
    }
    
    @Test
    public void setContentTypeMethod()
    {
        final PojoWithAnnotatedMethods pojo = new PojoWithAnnotatedMethods();
        final String expected = "application/octet-stream";
        AnnotationUtil.setContentType(pojo, expected);
        
        assertNotNull(pojo.getContentType());
        assertEquals(expected, pojo.getContentType());
    }
    
    @Test(expected = RuntimeException.class)
    public void getNonStringContentTypeField()
    {
        final Object o = new Object() {
            @RiakContentType 
            Date key;
        };
        
        AnnotationUtil.getContentType(o);
    }
    
    @Test(expected = RuntimeException.class)
    public void getNonStringContentTypeGetter()
    {
        final Object o = new Object() {
            @RiakContentType 
            Date getKey() { return null; }
            
            @RiakContentType
            void setKey(String key) {}
            
        };
        
        AnnotationUtil.getContentType(o);
    }
    
    @Test(expected = RuntimeException.class)
    public void getNonStringContentTypeSetter()
    {
        final Object o = new Object() {
            @RiakContentType 
            String getKey() { return null; }
            
            @RiakContentType
            void setKey(Date key) {}
            
        };
        
        AnnotationUtil.getContentType(o);
    }
    
    @Test
    public void noContentTypeFieldOrMethod()
    {
        final Object o = new Object();

        assertNull(AnnotationUtil.getContentType(o));
        assertNotNull(AnnotationUtil.getContentType(o, "default"));
        AnnotationUtil.setContentType(o, "default"); // should do nothing
    }
    
    @Test
    public void getNullContentTypeField()
    {
        final String expected = null;
        final PojoWithAnnotatedFields pojo = new PojoWithAnnotatedFields();
        pojo.contentType = expected;

        assertNull(AnnotationUtil.getContentType(pojo));
        assertNotNull(AnnotationUtil.getContentType(pojo, "default"));
    }
    
    @Test
    public void getNullContentTypeMethod()
    {
        final String expected = null;
        final PojoWithAnnotatedMethods pojo = new PojoWithAnnotatedMethods();
        pojo.setContentType(expected);

        assertNull(AnnotationUtil.getContentType(pojo));
        assertNotNull(AnnotationUtil.getContentType(pojo, "default"));
    }
    
    @Test
    public void setVTagField()
    {
        final String expected = "vtag";
        final PojoWithAnnotatedFields pojo = new PojoWithAnnotatedFields();
        AnnotationUtil.setVTag(pojo, expected);
        assertEquals(expected, pojo.vtag);
        assertEquals(expected, pojo.vtag);
    }
    
    @Test
    public void setVTagMethod()
    {
        final String expected = "vtag";
        final PojoWithAnnotatedMethods pojo = new PojoWithAnnotatedMethods();
        AnnotationUtil.setVTag(pojo, expected);
        assertEquals(expected, pojo.vtag);
        assertEquals(expected, pojo.vtag);
    }
    
    @Test(expected = RuntimeException.class)
    public void getNonStringVTagField()
    {
        final Object o = new Object() {
            @RiakVTag 
            Date key;
        };
        
        AnnotationUtil.setVTag(o, null);
    }
    
    @Test(expected = RuntimeException.class)
    public void getNonStringVTagSetter()
    {
        final Object o = new Object() {
            @RiakVTag 
            public void setVTag(Date vtag) {}
        };
        
        AnnotationUtil.setVTag(o, null);
    }
    
    protected static final String META_KEY_ONE = "metaKeyOne";
    
    protected static final class PojoWithAnnotatedFields
    {
        @RiakKey
        String key;
        
        @RiakBucketName
        String bucketName;
     
        @RiakBucketType
        String bucketType;
        
        @RiakVClock
        VClock vclock;
        
        @RiakUsermeta(key = META_KEY_ONE) 
        String metaItemOne;
        
        @RiakUsermeta
        Map<String,String> usermeta;
        
        @RiakIndex(name = "favorite_languages") 
        public Set<String> languages;

        @RiakIndex(name = "lucky_language")
        public String luckyLanguage;
        
        @RiakIndex(name = "lucky_long")
        public long luckyLong;
        
        @RiakIndex(name = "lucky_longlong")
        public Long luckyLongLong; 
        
        @RiakIndex(name = "longs")
        public Set<Long> longs;
        
        @RiakIndex(name = "raw_int")
        public byte[] rawInt;
        
        @RiakIndex(name = "raw_bin")
        public byte[] rawBin;
        
        @RiakIndex(name = "set_raw_bin")
        public Set<byte[]> rawBins;
        
        @RiakIndex(name = "set_raw_int")
        public Set<byte[]> rawInts;
        
        @RiakTombstone
        boolean tombstone;
        
        @RiakLinks
        Collection<RiakLink> links;
        
        @RiakContentType
        String contentType;
        
        @RiakVTag
        String vtag;
        
        @RiakLastModified
        Long lastModified;
        
        
    }
    
    protected static final class PojoWithAnnotatedByteFields
    {
        @RiakKey
        byte[] key;
        
        @RiakBucketName
        byte[] bucketName;
        
        @RiakBucketType
        byte[] bucketType;
        
        @RiakVClock
        byte[] vclock;
        
        @RiakTombstone
        Boolean tombstone;
    }
    
    
    protected static final class PojoWithAnnotatedMethods
    {
        private String key;
        private String bucketName;
        private String bucketType;
        private Map<String,String> usermeta;
        private String metaItemOne;
        private Set<Long> longs;
        private Set<String> strings;
        private long myLong;
        private Long myLongLong;
        private String myString;
        private VClock vClock;
        private boolean tombstone;
        private Collection<RiakLink> links;
        private String contentType;
        private String vtag;
        private long lastModified;
        private byte[] rawInt;
        private byte[] rawBin;
        private Set<byte[]> rawInts;
        private Set<byte[]> rawBins;
        
        @RiakKey
        public String getKey()
        {
            return this.key;
        }
        
        @RiakKey
        public void setKey(String key)
        {
            this.key = key;
        }
        
        @RiakBucketName
        public String getBucketName()
        {
            return bucketName;
        }
        
        @RiakBucketName
        public void setBucketName(String bucketName)
        {
            this.bucketName = bucketName;
        }
        
        @RiakBucketType
        public String getBucketType()
        {
            return bucketType;
        }
        
        @RiakBucketType
        public void setBucketType(String bucketType)
        {
            this.bucketType = bucketType;
        }
        
        @RiakVClock
        public VClock getVClock()
        {
            return this.vClock;
        }
        
        @RiakVClock
        public void setVClock(VClock vclock)
        {
            this.vClock = vclock;
        }
        
        @RiakUsermeta
        public Map<String, String> getUsermeta()
        {
            return this.usermeta;
        }
        
        @RiakUsermeta
        public void setUsermeta(Map<String,String> usermeta)
        {
            this.usermeta = usermeta;
        }
        
        @RiakUsermeta(key = META_KEY_ONE)
        public String getMetaItemOne() 
        {
            return this.metaItemOne;
        }
        
        @RiakUsermeta(key = META_KEY_ONE)
        public void setMetaItemOne(String item)
        {
            this.metaItemOne = item;
        }
        
        @RiakIndex(name = "longs")
        public Set<Long> getLongs()
        {
          return longs;
        }
        
        @RiakIndex(name = "longs")
        public void setLongs(Set<Long> longs)
        {
            this.longs = longs;
        }
 
        @RiakIndex(name = "strings") 
        public Set<String> getStrings()
        {
            return strings;
        }
        
        @RiakIndex(name = "strings") 
        public void setStrings(Set<String> strings)
        {
            this.strings = strings;
        }
        
        @RiakIndex(name = "long")
        public long getLong()
        {
          return myLong;
        }
        
        @RiakIndex(name = "long")
        public void setLong(long l)
        {
            this.myLong = l;
        }
        
        @RiakIndex(name = "longlong")
        public Long getLongLong()
        {
          return myLongLong;
        }
        
        @RiakIndex(name = "longlong")
        public void setLongLong(Long l)
        {
            this.myLongLong = l;
        }

        @RiakIndex(name = "string") 
        public String getString()
        {
            return myString;
        }
        
        @RiakIndex(name = "string") 
        public void setString(String s)
        {
            this.myString = s;
        }
        
        @RiakIndex(name ="raw_int")
        public void setRawIntIndex(byte[] bytes)
        {
            this.rawInt = bytes;
        }
        
        @RiakIndex(name ="raw_int")
        public byte[] getRawIntIndex()
        {
            return rawInt;
        }
        
        @RiakIndex(name ="set_raw_int")
        public void setRawIntsIndex(Set<byte[]> bytes)
        {
            this.rawInts = bytes;
        }
        
        @RiakIndex(name ="set_raw_int")
        public Set<byte[]> getRawIntsIndex()
        {
            return rawInts;
        }
        
        @RiakIndex(name ="raw_bin")
        public void setRawBinIndex(byte[] bytes)
        {
            this.rawBin = bytes;
        }
        
        @RiakIndex(name ="raw_bin")
        public byte[] getRawBinIndex()
        {
            return rawBin;
        }
        
        @RiakIndex(name ="set_raw_bin")
        public void setRawBinssIndex(Set<byte[]> bytes)
        {
            this.rawBins = bytes;
        }
        
        @RiakIndex(name ="set_raw_bin")
        public Set<byte[]> getRawBinsIndex()
        {
            return rawBins;
        }
        
        
        @RiakTombstone
        public Boolean getTombstone()
        {
            return this.tombstone;
        }
        
        @RiakTombstone
        public void setTombstone(Boolean tombstone)
        {
            this.tombstone = tombstone;
        }
        
        @RiakLinks
        public Collection<RiakLink> getLinks()
        {
            return this.links;
        }
        
        @RiakLinks
        public void setLinks(Collection<RiakLink> links)
        {
            this.links = links;
        }
        
        @RiakContentType
        public String getContentType()
        {
            return contentType;
        }
        
        @RiakContentType
        public void setContentType(String contentType)
        {
            this.contentType = contentType;
        }
        
        @RiakVTag
        public void setVTag(String vtag)
        {
            this.vtag = vtag;
        }
        
        @RiakLastModified
        public void setLastModified(Long lastModified)
        {
            this.lastModified = lastModified;
        }
    }
    
    protected static final class PojoWithAnnotatedByteMethods
    {
        private byte[] key;
        private byte[] bucketName;
        private byte[] bucketType;
        private byte[] vclock;
        private boolean tombstone;
        
        @RiakKey
        public byte[] getKey()
        {
            return this.key;
        }
        
        @RiakKey
        public void setKey(byte[] key)
        {
            this.key = key;
        }
        
        @RiakBucketName
        public byte[] getBucketName()
        {
            return bucketName;
        }
        
        @RiakBucketName
        public void setBucketName(byte[] bucketName)
        {
            this.bucketName = bucketName;
        }
        
        @RiakBucketType
        public void setBucketType(byte[] bucketType)
        {
            this.bucketType = bucketType;
        }
        
        @RiakBucketType
        public byte[] getBucketType()
        {
            return bucketType;
        }
        
        @RiakVClock
        public void setVClock(byte[] vclock)
        {
            this.vclock = vclock;
        }
        
        @RiakVClock
        public byte[] getVClock()
        {
            return vclock;
        }
        
        @RiakTombstone
        public boolean getTombstone()
        {
            return this.tombstone;
        }
        
        @RiakTombstone
        public void setTombstone(boolean tombstone)
        {
            this.tombstone = tombstone;
        }
        
    }
    
}

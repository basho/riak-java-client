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

import com.basho.riak.client.api.cap.VClock;
import com.basho.riak.client.core.query.UserMetadata.RiakUserMetadata;
import com.basho.riak.client.core.query.indexes.RiakIndexes;
import com.basho.riak.client.core.query.links.RiakLinks;
import com.basho.riak.client.core.util.BinaryValue;

/**
 * Singleton that wraps a cache of Class -> AnnotatioInfo and provides
 * convenience methods for getting and setting Riak annotated field values
 * 
 * @author russell
 * 
 */
public class AnnotationHelper
{

    private static final AnnotationHelper INSTANCE = new AnnotationHelper();
    private final AnnotationCache annotationCache = new AnnotationCache();

    private AnnotationHelper() {}

    public static AnnotationHelper getInstance()
    {
        return INSTANCE;
    }

    public <T> BinaryValue getRiakKey(T obj)
    {
        final AnnotationInfo annotationInfo = annotationCache.get(obj.getClass());
        return annotationInfo.getRiakKey(obj);
    }

    public <T> T setRiakKey(T obj, BinaryValue key)
    {
        final AnnotationInfo annotationInfo = annotationCache.get(obj.getClass());
        annotationInfo.setRiakKey(obj, key);
        
        return obj;
    }

    public <T> BinaryValue getRiakBucketName(T obj)
    {
        final AnnotationInfo annotationInfo = annotationCache.get(obj.getClass());
        return annotationInfo.getRiakBucketName(obj);
    }
    
    public <T> T setRiakBucketName(T obj, BinaryValue bucketName)
    {
        final AnnotationInfo annotationInfo = annotationCache.get(obj.getClass());
        annotationInfo.setRiakBucketName(obj, bucketName);
        return obj;
    }
    
    public <T> BinaryValue getRiakBucketType(T obj)
    {
        final AnnotationInfo annotationInfo = annotationCache.get(obj.getClass());
        return annotationInfo.getRiakBucketType(obj);
    }
    
    public <T> T setRiakBucketType(T obj, BinaryValue bucketType)
    {
        final AnnotationInfo annotationInfo = annotationCache.get(obj.getClass());
        annotationInfo.setRiakBucketType(obj, bucketType);
        return obj;
    }
    
    public <T> boolean hasRiakVClockAnnotation(T obj)
    {
        final AnnotationInfo annotationInfo = annotationCache.get(obj.getClass());
        return annotationInfo.hasRiakVClock();
    }
    
    public <T> T setRiakVClock(T obj, VClock vclock)
    {
        final AnnotationInfo annotationInfo = annotationCache.get(obj.getClass());
        annotationInfo.setRiakVClock(obj, vclock);
        
        return obj;
    }
    
    public <T> VClock getRiakVClock(T obj)
    {
        final AnnotationInfo annotationInfo = annotationCache.get(obj.getClass());
        return annotationInfo.getRiakVClock(obj);
    }
    
    public <T> T setRiakTombstone(T obj, boolean isTombstone)
    {
        final AnnotationInfo annotationInfo = annotationCache.get(obj.getClass());
        annotationInfo.setRiakTombstone(obj, isTombstone);
        
        return obj;
    }
    
    public <T> Boolean getRiakTombstone(T obj)
    {
        final AnnotationInfo annotationInfo = annotationCache.get(obj.getClass());
        Boolean tombstone = annotationInfo.getRiakTombstone(obj);
        
        return tombstone;
    }
    
    public <T> T setRiakContentType(T obj, String contentType)
    {
        final AnnotationInfo annotationInfo = annotationCache.get(obj.getClass());
        annotationInfo.setRiakContentType(obj, contentType);
        return obj;
    }
    
    public <T> String getRiakContentType(T obj)
    {
        final AnnotationInfo annotationInfo = annotationCache.get(obj.getClass());
        return annotationInfo.getRiakContentType(obj);
    }
    
    public <T> T setRiakLastModified(T obj, Long lastModified)
    {
        final AnnotationInfo annotationInfo = annotationCache.get(obj.getClass());
        annotationInfo.setRiakLastModified(obj, lastModified);
        return obj;
    }
    
    public <T> T setRiakVTag(T obj, String vtag)
    {
        final AnnotationInfo annotationInfo = annotationCache.get(obj.getClass());
        annotationInfo.setRiakVTag(obj, vtag);
        return obj;
    }
    
    public <T> RiakUserMetadata getUsermetaData(RiakUserMetadata container, T obj)
    {
        final AnnotationInfo annotationInfo = annotationCache.get(obj.getClass());
        return annotationInfo.getUsermetaData(container, obj);
    }

    public <T> T setUsermetaData(RiakUserMetadata usermetaData, T obj)
    {
        final AnnotationInfo annotationInfo = annotationCache.get(obj.getClass());
        annotationInfo.setUsermetaData(usermetaData, obj);
        return obj;
    }

    public <T> RiakIndexes getIndexes(RiakIndexes container, T obj)
    {
        final AnnotationInfo annotationInfo = annotationCache.get(obj.getClass());
        return annotationInfo.getIndexes(container, obj);
    }

    public <T> T setIndexes(RiakIndexes indexes, T obj)
    {
        final AnnotationInfo annotationInfo = annotationCache.get(obj.getClass());
        annotationInfo.setIndexes(indexes, obj);
        return obj;
    }

    public <T> RiakLinks getLinks(RiakLinks container, T obj)
    {
        final AnnotationInfo annotationInfo = annotationCache.get(obj.getClass());
        return annotationInfo.getLinks(container, obj);
    }

    public <T> T setLinks(RiakLinks links, T obj)
    {
        final AnnotationInfo annotationInfo = annotationCache.get(obj.getClass());
        annotationInfo.setLinks(links, obj);
        return obj;
    }
}

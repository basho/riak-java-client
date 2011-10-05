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
package com.basho.riak.client.convert.reflect;

import java.util.Map;

/**
 * Singleton that wraps a cache of Class -> AnnotatioInfo and provides
 * convenience methods for getting and setting Riak annotated field values
 * 
 * @author russell
 * 
 */
public class AnnotationHelper {

    private static final AnnotationHelper INSTANCE = new AnnotationHelper();

    private AnnotationCache annotationCache = new AnnotationCache();

    private AnnotationHelper() {}

    public static AnnotationHelper getInstance() {
        return INSTANCE;
    }

    public <T> String getRiakKey(T obj) {
        String key = null;
        final AnnotationInfo annotationInfo = annotationCache.get(obj.getClass());

        if (annotationInfo.hasRiakKey()) {
            key = annotationInfo.getRiakKey(obj);
        }

        return key;
    }

    public <T> T setRiakKey(T obj, String key) {
        final AnnotationInfo annotationInfo = annotationCache.get(obj.getClass());

        if (annotationInfo.hasRiakKey()) {
            annotationInfo.setRiakKey(obj, key);
        }

        return obj;
    }

    public <T> Map<String, String> getUsermetaData(T obj) {
        final AnnotationInfo annotationInfo = annotationCache.get(obj.getClass());
        return annotationInfo.getUsermetaData(obj);
    }

    public <T> T setUsermetaData(Map<String, String> usermetaData, T obj) {
        final AnnotationInfo annotationInfo = annotationCache.get(obj.getClass());
        annotationInfo.setUsermetaData(usermetaData, obj);
        return obj;
    }

}

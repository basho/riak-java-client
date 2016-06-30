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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

/**
 * TODO: consider class reloading and re-scanning
 * @author russell
 * 
 */
public class AnnotationCache
{
    @SuppressWarnings("rawtypes") private final ConcurrentHashMap<Class, Future<AnnotationInfo>> cache = new ConcurrentHashMap<Class, Future<AnnotationInfo>>();

    /**
     * @param clazz the class to be scanned and cached.
     * @return an AnnotationInfo instance.
     */
    public <T> AnnotationInfo get(Class<T> clazz)
    {
        Future<AnnotationInfo> scanner = cache.get(clazz);

        if (scanner == null)
        {
            FutureTask<AnnotationInfo> scannerTask = new FutureTask<AnnotationInfo>(new AnnotationScanner(clazz));

            scanner = cache.putIfAbsent(clazz, scannerTask);
            if (scanner == null)
            {
                scanner = scannerTask;
                scannerTask.run();
            }
        }

        try
        {
            return scanner.get();
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (ExecutionException e)
        {
            cache.remove(clazz);
            throw new RuntimeException(e.getCause());
        }
    }

}

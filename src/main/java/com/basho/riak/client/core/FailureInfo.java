/*
 * Copyright 2014 Brian Roach <roach at basho dot com>.
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

package com.basho.riak.client.core;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class FailureInfo<T>
{
    private final Throwable cause;
    private final T queryInfo;
    
    public FailureInfo(Throwable cause, T queryInfo)
    {
        this.cause = cause;
        this.queryInfo = queryInfo;
    }
    
    /**
     * The exception thrown during the execution of the operation.
     * @return the exception.
     */
    public Throwable getCause()
    {
        return cause;
    }
    
    /**
     * Return information about the specific query that failed.
     * <p>
     * During an async operation this is useful to determine which
     * operation failed. For example, a failed fetch will provide the
     * {@code Location} allowing you to know what query failed.
     * </p>
     * @return Infomation pertaining to the failed operation
     */
    public T getQueryInfo()
    {
        return queryInfo;
    }
}

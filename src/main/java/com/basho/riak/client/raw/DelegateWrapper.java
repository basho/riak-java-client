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
package com.basho.riak.client.raw;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class DelegateWrapper
{
    private final RawClient client;
    private final AtomicBoolean isBad;
    private Exception lastException;

    public DelegateWrapper(RawClient client)
    {
        this.client = client;
        isBad = new AtomicBoolean(false);
    }

    public boolean isBad()
    {
        return isBad.get();
    }

    /**
     * This is an atomic operation and will return true only if the 
     * current thread succeeded in toggling the flag. 
     * 
     * @return true if the operation succeeded in flagging the delegate as bad,
     * false otherwise
     */
    public boolean markAsBad()
    {
        return isBad.compareAndSet(false, true);
    }

    /**
     * This is an atomic operation and will return true only if the 
     * current thread succeeded in toggling the flag. 
     * 
     * @return true if the operation succeeded in flagging the delegate as good,
     * false otherwise
     */
    public boolean markAsGood()
    {
        return isBad.compareAndSet(true, false);
    }
    
    public RawClient getClient()
    {
        return client;
    }
    
    public void setException(Exception e)
    {
        this.lastException = e;
    }
    
    public Exception getException()
    {
        return lastException;
    }
    
}

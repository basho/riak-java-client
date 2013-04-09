/*
 * Copyright 2013 Basho Technologies, Inc.
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

import java.util.concurrent.Semaphore;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class AdjustableSemaphore extends Semaphore
{
    private static final long serialVersionUID = -5118488872281021072L;
    private volatile int maxPermits;
    
    public AdjustableSemaphore(int numPermits)
    {
        super(numPermits);
        this.maxPermits = numPermits;
    }
    
    public AdjustableSemaphore(int numPermits, boolean fair)
    {
        super(numPermits, fair);
        this.maxPermits = numPermits;
    }
    
    public int getMaxPermits()
    {
        return maxPermits;
    }
    
    // Synchronized because we're (potentially) changing this.maxPermits
    synchronized void setMaxPermits(int maxPermits)
    {
        int diff = maxPermits - this.maxPermits;
        
        if (diff == 0)
        {
            return;
        }
        else if (diff > 0)
        {
            release(diff);
        }
        else if (diff < 0)
        {
            reducePermits(diff);
        }
        
        this.maxPermits = maxPermits;
    }
    
}

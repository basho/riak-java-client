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

import java.util.concurrent.TimeUnit;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class UnlimitedSemaphore extends AdjustableSemaphore
{
    private static final long serialVersionUID = 7339318135287804019L;
    public UnlimitedSemaphore() {
        super(0);
    }
    
    @Override
    public void release()
    {
        // No-Op
    }
    
    @Override
    public void release(int permits)
    {
        // No-op
    }
    
    @Override
    public boolean tryAcquire(int permits, long timeout, TimeUnit unit)
    {
        return true;
    }
    
    @Override
    public boolean tryAcquire(long timeout, TimeUnit unit)
    {
        return true;
    }
    
    @Override
    public boolean tryAcquire(int permits)
    {
        return true;
    }
    
    @Override
    public boolean tryAcquire()
    {
        return true;
    }
    
    @Override
    public void acquire()
    {
        // No-op
    }
    
    @Override 
    public void acquire(int permits)
    {
        // No-Op
    }
    
    @Override
    public void setMaxPermits(int permits)
    {
        // No=Op
    }
    
    @Override 
    public int getMaxPermits()
    {
        return Integer.MAX_VALUE;
    }
    
    @Override
    public int availablePermits()
    {
        return 1;
    }
    
    @Override
    public void acquireUninterruptibly()
    {
        // No-op
    }
    
    @Override
    public void acquireUninterruptibly(int permits)
    {
        // No-Op
    }
    
    
}

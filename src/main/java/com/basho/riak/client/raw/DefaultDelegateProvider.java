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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class DefaultDelegateProvider implements DelegateProvider
{
    final ArrayList<DelegateWrapper> delegates;
    private int currentIndex = 0;
    private final ReentrantLock lock = new ReentrantLock(); 
    private Timer checker;
    private volatile boolean keepChecking =  true;
    private long healthCheckDelayMillis = 5000L;
    
    public DefaultDelegateProvider()
    {
        delegates = new ArrayList<DelegateWrapper>();
    }
    
    /**
     * Sets the delay for the background healthcheck that tests nodes that are
     * marked as bad. 
     * Note that this has no effect once init() has been called.
     * @param delayInMillis - number of milliseconds to wait between checks
     */
    public void setHealthCheckDelayMillis(long delayInMillis)
    {
        this.healthCheckDelayMillis = delayInMillis;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.DelegateProvider#init(final RawClient[] clients)
     */
    public void init(final RawClient[] clients)
    {
        lock.lock();
        try
        {
            for (int i = 0; i < clients.length; i++)
            {
                delegates.add(i, new DelegateWrapper(clients[i]));
            }
        }
        finally
        {
            lock.unlock();
        }
        
        checker = new Timer();
        checker.schedule(new HealthCheck(healthCheckDelayMillis), healthCheckDelayMillis);
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.DelegateProvider#getDelegate()
     */
    public DelegateWrapper getDelegate() throws NoDelegatesAvailableException
    {
        lock.lock();
        try 
        {
            for (int i = 0; i < delegates.size(); i++)
            {
                int index = Math.abs(currentIndex % delegates.size());
                currentIndex++;
                if (!delegates.get(index).isBad())
                {
                    return delegates.get(index);
                }
            }
        
            // tried every delegate, they're all bad
            throw new NoDelegatesAvailableException(getAllDelegates());
        
        }
        finally 
        {
            lock.unlock();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.DelegateProvider#markAsBad(DelegateWrapper delegate, Exception e)
     */
    public void markAsBad(DelegateWrapper delegate, Exception e)
    {
        if (delegate.markAsBad())
        {
            delegate.setException(e);
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.DelegateProvider#markAsGood(DelegateWrapper delegate)
     */
    private void markAsGood(DelegateWrapper delegate)
    {
        if (delegate.markAsGood())
        {
            delegate.setException(null);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.DelegateProvider#getAllDelegates()
     */
    public Collection<DelegateWrapper> getAllDelegates()
    {
        return Collections.unmodifiableCollection(delegates);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.DelegateProvider#stop()
     */
    public void stop()
    {
        keepChecking = false;
        checker.cancel();
    }

    private class HealthCheck extends TimerTask
    {
        private final long delay;
        
        public HealthCheck(long delay)
        {
            this.delay = delay;
        }
        
        public void run()
        {
            for (int i = 0; i < delegates.size(); i++)
            {
                if (delegates.get(i).isBad())
                {
                    try
                    {
                        delegates.get(i).getClient().ping();
                        markAsGood(delegates.get(i));
                    }
                    catch (IOException e)
                    {
                        // no-op ... still bad
                    }
                }
            }

            if (keepChecking)
                checker.schedule(this, delay);
        }
    }
    
    
}



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
package com.basho.riak.client.raw.pbc;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.basho.riak.pbc.RiakConnectionPool;

/**
 * Wraps a semaphore for the cluster and a semaphore for the pool. Used by
 * {@link RiakConnectionPool}
 * 
 * @author russell
 */
public class PoolSemaphore extends Semaphore {

    /**
     * Eclipse generated
     */
    private static final long serialVersionUID = 7060018376512789254L;

    private final Semaphore clusterSemaphore;
    private final Semaphore poolSemaphore;

    /**
     * Creates a pool {@link Semaphore} with <code>permits</code> permits and
     * <code>fair</code> fairness. Calls to tryAcquire(long, TimeUnit),
     * release() and tryAcquire(int) call both <code>cluster</code> and pool
     * {@link Semaphore}
     * 
     * @param cluster
     *            a {@link Semaphore} configured for the total cluster maximum
     *            number of connections
     * @param permits
     *            maximum connections for the pool
     * @param fair
     *            is the {@link Semaphore} fair (see {@link Semaphore})
     */
    public PoolSemaphore(Semaphore cluster, int permits, boolean fair) {
        super(permits, fair);
        clusterSemaphore = cluster;
        poolSemaphore = RiakConnectionPool.getSemaphore(permits);
    }

    /**
     * Both semaphores are released
     */
    @Override public void release() {
        clusterSemaphore.release();
        poolSemaphore.release();
    }

    /**
     * Tries to acquire on the cluster semaphore, and only if that succeeds does
     * it try the pool semaphore Warning: this means potentially double the
     * timeout
     * 
     * TODO configure option for timeout to be shared (i.e. pool semaphore gets
     * remaining time after cluster acquire)
     * 
     * @see java.util.concurrent.Semaphore#tryAcquire(long,
     *      java.util.concurrent.TimeUnit)
     */
    @Override public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
        boolean clusterPermitted = clusterSemaphore.tryAcquire(timeout, unit);
        boolean poolPermitted = false;

        if (clusterPermitted) {
            poolPermitted = poolSemaphore.tryAcquire(timeout, unit);
        }

        if (poolPermitted) {
            return true;
        } else {
            clusterSemaphore.release();
            return false;
        }
    }

    /**
     * Tries to acquire from the cluster semaphore, and only if that succeeds
     * does it try the pool semaphore.
     * 
     * @see java.util.concurrent.Semaphore#tryAcquire(int)
     */
    @Override public boolean tryAcquire(int permits) {
        boolean clusterPermitted = clusterSemaphore.tryAcquire(permits);
        boolean poolPermitted = false;

        if (clusterPermitted) {
            poolPermitted = poolSemaphore.tryAcquire(permits);
        }

        if (poolPermitted) {
            return true;
        } else {
            clusterSemaphore.release(permits);
            return false;
        }
    }
}

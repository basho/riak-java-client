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
package com.basho.riak.pbc;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Not really a semaphore at all. Always returns at once, has no state. For the
 * case where you may or may not need a semaphore and you can make that choice
 * once rather than at every semaphore acquire/release point. Used by
 * {@link RiakConnectionPool} to implement a boundless pool.
 * 
 * @author russell
 * 
 */
public class LimitlessSemaphore extends Semaphore {

    /**
     * 
     */
    private static final long serialVersionUID = -4752538034544754767L;
    public LimitlessSemaphore() {
        super(0);
    }

    /**
     * A NO-OP
     */
    @Override public void release() {
        // NO-OP;
    }

    /**
     * Always returns true at once.
     */
    @Override public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
        return true;
    }
}

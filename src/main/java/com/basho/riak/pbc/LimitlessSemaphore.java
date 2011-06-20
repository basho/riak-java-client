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
 * @author russell
 *
 */
public class LimitlessSemaphore extends Semaphore {

    /**
     * 
     */
    private static final long serialVersionUID = -4752538034544754767L;

    /**
     * @param arg0
     */
    public LimitlessSemaphore(int arg0) {
        super(arg0);
    }

    /* (non-Javadoc)
     * @see java.util.concurrent.Semaphore#release()
     */
    @Override public void release() {
        //NO-OP;
    }

    /* (non-Javadoc)
     * @see java.util.concurrent.Semaphore#tryAcquire(int, long, java.util.concurrent.TimeUnit)
     */
    @Override public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
        return true;
    }
    
    

}

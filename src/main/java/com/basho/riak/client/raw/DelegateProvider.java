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

import com.basho.riak.client.raw.http.HTTPClusterClient;
import com.basho.riak.client.raw.pbc.PBClusterClient;
import java.util.Collection;

/**
 *
 * Implement this and pass to a {@link com.basho.riak.client.raw.config.ClusterConfig}
 * for providing delegates to Riak operations.
 * 
 * @author Brian Roach <roach at basho dot com>
 */
public interface DelegateProvider
{
    /**
     * Inject the set of {@link RawClient} instances to be used as a cluster.
     * Also allows for the starting of background threads inside the DelegateProvider
     * This is called by the constructor of both the {@link HTTPClusterClient} and
     * {@link PBClusterClient}.
     * 
     * @param clients 
     */
    public void init(RawClient[] clients);
    
    /**
     * Returns a {@link RawClient} wrapped in a {@link DelegateWrapper} to a
     * {@link ClusterClient} instance every time a Riak operation is performed.
     * 
     * @return The wrapped RawClient to be used 
     * @throws NoDelegatesAvailableException - all DelegateWrappers have been 
     * marked as bad (have thrown IOExceptions) and none are available for use
     */
    public DelegateWrapper getDelegate() throws NoDelegatesAvailableException;
    
    /**
     * Allows a {@link ClusterClient} to report a delegate as having thrown 
     * an exception.
     * @param delegate
     * @param e - the exception that was thrown (for informational purposes) 
     */
    public void markAsBad(DelegateWrapper delegate, Exception e);
    
    /**
     * Convenience method to retrieve the entire set of delegates
     * @return A {@link Collection} of DelegateWrappers
     */
    public Collection<DelegateWrapper> getAllDelegates();
    
    /**
     * Called by the shutdown() method of both the {@link HTTPClusterClient} and
     * {@link PBClusterClient}.
     */
    public void stop();
}

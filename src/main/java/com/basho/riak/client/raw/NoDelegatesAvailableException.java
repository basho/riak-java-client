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
import java.util.Collection;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class NoDelegatesAvailableException extends IOException
{
    private final Collection<DelegateWrapper> delegates;
    
    public NoDelegatesAvailableException(Collection<DelegateWrapper> delegates)
    {
        super("No delegates available");
        this.delegates = delegates;
    }
    
    public NoDelegatesAvailableException(String message, Collection<DelegateWrapper> delegates)
    {
        super(message);
        this.delegates = delegates;
    }
    
    public Collection<DelegateWrapper> getDelegates()
    {
        return delegates;
    }
    
}

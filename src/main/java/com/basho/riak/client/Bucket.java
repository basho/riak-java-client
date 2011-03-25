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
package com.basho.riak.client;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author russell
 * 
 */
public interface Bucket {

    StoreOperation store(String key, String value);

    StoreOperation store(RiakObject o);

    FetchOperation fetch(String key);

    FetchOperation fetch(RiakObject o);

    DeleteOperation delete(RiakObject o);
    
    
    String getName();
    boolean isAllowSiblings();
    boolean isLastWriteWins();
    int getNVal();
    String getBackend();
    int getSmallVClock();
    int getBigVClock();
    long getYoungVClock();
    long getOldVClock();
    Collection<NamedFunction> getPrecommitHooks();
    Collection<NamedErlangFunction> getPostCommitHooks();
    NamedErlangFunction getChashKeyFunction();
    NamedErlangFunction getLinkWalkFunction();
    int getR();
    int getW();
    int getDW();
    int getRW();
    
    // iterate the keys
    Iterator<String> iterator();

}

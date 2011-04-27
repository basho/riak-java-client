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
package com.basho.riak.newapi.query.filter;

import java.util.Collection;
import java.util.LinkedList;

/**
 * @author russell
 *
 */
public abstract class AbstractLogicalFilter implements LogicalFilter {


    private final Collection<Object[]> filters = new LinkedList<Object[]>();

    public AbstractLogicalFilter(KeyFilter... filters) {
        synchronized (this.filters) {
            for (KeyFilter filter : filters) {
                this.filters.add(filter.asArray());
            }
        }
    }

    public AbstractLogicalFilter add(KeyFilter filter) {
        synchronized (filter) {
            filters.add(filter.asArray());
        }
        return this;
    }

    public Object[] asArray() {

        int length = 0;
        synchronized (filters) {
            length = filters.size();
        }

        final Object[] merged = new Object[length + 1];
        merged[0] = getName();

        synchronized (filters) {
            System.arraycopy(filters.toArray(), 0, merged, 1, filters.size());
        }

        return merged;
    }
    
    protected abstract String getName();

}

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
package com.megacorp.commerce;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.basho.riak.client.convert.RiakKey;

/**
 * A simple domain object for the sake of ITests.
 * 
 * @author russell
 * 
 */
public class ShoppingCart implements Iterable<String> {

    @RiakKey private final String userId;
    @JsonProperty private final Set<String> items;

    /**
     * @param userId
     */
    @JsonCreator public ShoppingCart(@JsonProperty("userId") String userId) {
        this.userId = userId;
        items = new CopyOnWriteArraySet<String>();
    }

    public ShoppingCart addItem(String item) {
        items.add(item);
        return this;
    }

    public ShoppingCart addItems(Collection<String> all) {
        items.addAll(all);
        return this;
    }

    public ShoppingCart removeItem(String item) {
        items.remove(item);
        return this;
    }

    public ShoppingCart clear() {
        items.clear();
        return this;
    }

    public int size() {
        return items.size();
    }

    public boolean hasItem(String item) {
        return items.contains(item);
    }

    public boolean hasAll(Collection<String> all) {
        return items.containsAll(all);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Iterable#iterator()
     */
    public Iterator<String> iterator() {
        return items.iterator();
    }

    public String getUserId() {
        return userId;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((items == null) ? 0 : items.hashCode());
        result = prime * result + ((userId == null) ? 0 : userId.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof ShoppingCart)) {
            return false;
        }
        ShoppingCart other = (ShoppingCart) obj;
        if (items == null) {
            if (other.items != null) {
                return false;
            }
        } else if (!items.equals(other.items)) {
            return false;
        }
        if (userId == null) {
            if (other.userId != null) {
                return false;
            }
        } else if (!userId.equals(other.userId)) {
            return false;
        }
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override public String toString() {
        return String.format("ShoppingCart [userId=%s, items=%s]", userId, items);
    }

}

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

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @author russell
 * 
 */
public class LegacyCart {

    private String userId;
    private Set<String> cartItems = new CopyOnWriteArraySet<String>();

    /**
     * @return the userId
     */
    public String getUserId() {
        return userId;
    }

    /**
     * @param userId
     *            the userId to set
     */
    public void setUserId(String userId) {
        this.userId = userId;
    }

    /**
     * @return the cartItems
     */
    public Set<String> getCartItems() {
        return cartItems;
    }

    /**
     * @param cartItems
     *            the cartItems to set
     */
    public void setCartItems(Set<String> cartItems) {
        this.cartItems = cartItems;
    }

    /**
     * @param string
     */
    public void addItem(String item) {
        cartItems.add(item);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((cartItems == null) ? 0 : cartItems.hashCode());
        result = prime * result + ((userId == null) ? 0 : userId.hashCode());
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof LegacyCart)) {
            return false;
        }
        LegacyCart other = (LegacyCart) obj;
        if (cartItems == null) {
            if (other.cartItems != null) {
                return false;
            }
        } else if (!cartItems.equals(other.cartItems)) {
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
}

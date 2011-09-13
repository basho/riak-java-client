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
package com.basho.riak.client.cap;

import com.basho.riak.client.builders.BucketPropertiesBuilder;

/**
 * Encapsulates a buckets r/w/dw/rw/p/pr/pw quora as either the symbolic Quora or an
 * int.
 * <p>
 * </p>
 * @see BucketPropertiesBuilder
 * @author russell
 */
public final class Quorum {
    private final int i;
    private final Quora quorum;

    /**
     * Construct an instance that wraps an int value
     * 
     * @param i
     */
    public Quorum(int i) {
        this.quorum = Quora.INTEGER;
        this.i = i;
    }

    /**
     * Construct and instance that wraps a symbolic Quora
     * 
     * @param quorum
     */
    public Quorum(Quora quorum) {
        this.quorum = quorum;
        this.i = quorum.getValue();
    }

    public boolean isSymbolic() {
        return !Quora.INTEGER.equals(quorum);
    }

    /**
     * The int value of the quorum. Call isSymbolic to determine if you should
     * use this.
     * 
     * @return the int value. Will be a negative number for symbolic values.
     */
    public int getIntValue() {
        return this.i;
    }

    /**
     * Get the Symbolic value of this quorum.
     * 
     * A value of {@link Quora#INTEGER} means that the quorum has a meaningful
     * int value
     * 
     * @return
     */
    public Quora getSymbolicValue() {
        return quorum;
    }

    /**
     * @return the name given to this quorum's symbolic value
     * @see Quora#getName()
     */
    public String getName() {
        return quorum.getName();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + i;
        result = prime * result + ((quorum == null) ? 0 : quorum.hashCode());
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
        if (!(obj instanceof Quorum)) {
            return false;
        }
        Quorum other = (Quorum) obj;
        if (i != other.i) {
            return false;
        }
        if (quorum != other.quorum) {
            return false;
        }
        return true;
    }
}
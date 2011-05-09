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

/**
 * TODO needs further definition and accessor methods.
 *
 * @author russell
 */
public final class Quorum {
    private Integer i;
    private Quora quorum;

    public Quorum(int i) {
        this.i = i;
    }

    public Quorum(Quora quorum) {
        this.quorum = quorum;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((i == null) ? 0 : i.hashCode());
        result = prime * result + ((quorum == null) ? 0 : quorum.hashCode());
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
        if (!(obj instanceof Quorum)) {
            return false;
        }
        Quorum other = (Quorum) obj;
        if (i == null) {
            if (other.i != null) {
                return false;
            }
        } else if (!i.equals(other.i)) {
            return false;
        }
        if (quorum != other.quorum) {
            return false;
        }
        return true;
    }
}
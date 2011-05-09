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
package com.basho.riak.client.bucket;

import java.util.Collection;

import com.basho.riak.client.builders.BucketPropertiesBuilder;
import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.query.functions.NamedErlangFunction;
import com.basho.riak.client.query.functions.NamedFunction;

/**
 * Since not all interfaces to Riak are equal in terms of what they provide not
 * all RawClients can be expected to set all values. Which means that *any* of
 * the getters may return null.
 * 
 * @author russell
 */
public class DefaultBucketProperties implements BucketProperties {

    private final Boolean allowSiblings;
    private final Boolean lastWriteWins;
    private final Integer nVal;
    private final String backend;
    private final Integer smallVClock;
    private final Integer bigVClock;
    private final Long youngVClock;
    private final Long oldVClock;
    private final Collection<NamedFunction> precommitHooks;
    private final Collection<NamedErlangFunction> postcommitHooks;
    private final Quorum r;
    private final Quorum w;
    private final Quorum dw;
    private final Quorum rw;
    private final NamedErlangFunction chashKeyFunction;
    private final NamedErlangFunction linkWalkFunction;

    /**
     * @param allowSiblings
     * @param lastWriteWins
     * @param nVal
     * @param backend
     * @param smallVClock
     * @param bigVClock
     * @param youngVClock
     * @param oldVClock
     * @param precommitHooks
     * @param postcommitHooks
     * @param r
     * @param w
     * @param dw
     * @param rw
     * @param chashKeyFunction
     * @param linkWalkFunction
     */
    public DefaultBucketProperties(final BucketPropertiesBuilder builder) {
        this.allowSiblings = builder.allowSiblings;
        this.lastWriteWins = builder.lastWriteWins;
        this.nVal = builder.nVal;
        this.backend = builder.backend;
        this.smallVClock = builder.smallVClock;
        this.bigVClock = builder.bigVClock;
        this.youngVClock = builder.youngVClock;
        this.oldVClock = builder.oldVClock;
        this.precommitHooks = builder.precommitHooks;
        this.postcommitHooks = builder.postcommitHooks;
        this.r = builder.r;
        this.w = builder.w;
        this.dw = builder.dw;
        this.rw = builder.rw;
        this.chashKeyFunction = builder.chashKeyFunction;
        this.linkWalkFunction = builder.linkWalkFunction;
    }

    /**
     * @return the allowSiblings if set, or null if not
     */
    public Boolean getAllowSiblings() {
        return allowSiblings;
    }

    /**
     * @return the lastWriteWins if set or null if not
     */
    public Boolean getLastWriteWins() {
        return lastWriteWins;
    }

    /**
     * @return the nVal if set or null if not
     */
    public Integer getNVal() {
        return nVal;
    }

    /**
     * @return the backend if set, or null.
     */
    public String getBackend() {
        return backend;
    }

    /**
     * 
     * @return the small vclock pruning property if set, or null.
     */
    public int getSmallVClock() {
        return smallVClock;
    }

    /**
     * 
     * @return the big vclock pruning size property if set, or null.
     */
    public int getBigVClock() {
        return bigVClock;
    }

    /**
     * 
     * @return the young vclock prune property if set, or null.
     */
    public long getYoungVClock() {
        return youngVClock;
    }

    /**
     * 
     * @return the old vclock prune property if set, or null
     */
    public long getOldVClock() {
        return oldVClock;
    }

    /**
     * @return the pre commit hooks, if any, or an empty collection.
     */
    public Collection<NamedFunction> getPrecommitHooks() {
        return precommitHooks;
    }

    /**
     * @return the post commit hooks, if ant, or an empty collection.
     */
    public Collection<NamedErlangFunction> getPostcommitHooks() {
        return postcommitHooks;
    }

    /**
     * 
     * @return the default CAP read quorum for this bucket, or null.
     */
    public Quorum getR() {
        return r;
    }

    /**
     * 
     * @return the default CAP write quorum for this bucket, or null.
     */
    public Quorum getW() {
        return w;
    }

    /**
     * 
     * @return the default CAP RW (delete) quorum for this bucket, or null.
     */
    public Quorum getRW() {
        return rw;
    }

    /**
     * 
     * @return the default CAP durable write quorum for this bucket, or null.
     */
    public Quorum getDW() {
        return dw;
    }

    /**
     * @return the key hashing function for the bucket, or null.
     */
    public NamedErlangFunction getChashKeyFunction() {
        return chashKeyFunction;
    }

    /**
     * @return the link walking function for the bucket, or null.
     */
    public NamedErlangFunction getLinkWalkFunction() {
        return linkWalkFunction;
    }

    /**
     * 
     * @return a Builder populated from this BucketProperties' values.
     */
    public BucketPropertiesBuilder fromMe() {
        return DefaultBucketProperties.from(this);
    }

    /**
     * 
     * @param properties
     * @return a Builder populated with properties values.
     */
    public static BucketPropertiesBuilder from(DefaultBucketProperties properties) {
        return BucketPropertiesBuilder.from(properties);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((allowSiblings == null) ? 0 : allowSiblings.hashCode());
        result = prime * result + ((backend == null) ? 0 : backend.hashCode());
        result = prime * result + ((bigVClock == null) ? 0 : bigVClock.hashCode());
        result = prime * result + ((chashKeyFunction == null) ? 0 : chashKeyFunction.hashCode());
        result = prime * result + ((dw == null) ? 0 : dw.hashCode());
        result = prime * result + ((lastWriteWins == null) ? 0 : lastWriteWins.hashCode());
        result = prime * result + ((linkWalkFunction == null) ? 0 : linkWalkFunction.hashCode());
        result = prime * result + ((nVal == null) ? 0 : nVal.hashCode());
        result = prime * result + ((oldVClock == null) ? 0 : oldVClock.hashCode());
        result = prime * result + ((postcommitHooks == null) ? 0 : postcommitHooks.hashCode());
        result = prime * result + ((precommitHooks == null) ? 0 : precommitHooks.hashCode());
        result = prime * result + ((r == null) ? 0 : r.hashCode());
        result = prime * result + ((rw == null) ? 0 : rw.hashCode());
        result = prime * result + ((smallVClock == null) ? 0 : smallVClock.hashCode());
        result = prime * result + ((w == null) ? 0 : w.hashCode());
        result = prime * result + ((youngVClock == null) ? 0 : youngVClock.hashCode());
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
        if (!(obj instanceof DefaultBucketProperties)) {
            return false;
        }
        DefaultBucketProperties other = (DefaultBucketProperties) obj;
        if (allowSiblings == null) {
            if (other.allowSiblings != null) {
                return false;
            }
        } else if (!allowSiblings.equals(other.allowSiblings)) {
            return false;
        }
        if (backend == null) {
            if (other.backend != null) {
                return false;
            }
        } else if (!backend.equals(other.backend)) {
            return false;
        }
        if (bigVClock == null) {
            if (other.bigVClock != null) {
                return false;
            }
        } else if (!bigVClock.equals(other.bigVClock)) {
            return false;
        }
        if (chashKeyFunction == null) {
            if (other.chashKeyFunction != null) {
                return false;
            }
        } else if (!chashKeyFunction.equals(other.chashKeyFunction)) {
            return false;
        }
        if (dw == null) {
            if (other.dw != null) {
                return false;
            }
        } else if (!dw.equals(other.dw)) {
            return false;
        }
        if (lastWriteWins == null) {
            if (other.lastWriteWins != null) {
                return false;
            }
        } else if (!lastWriteWins.equals(other.lastWriteWins)) {
            return false;
        }
        if (linkWalkFunction == null) {
            if (other.linkWalkFunction != null) {
                return false;
            }
        } else if (!linkWalkFunction.equals(other.linkWalkFunction)) {
            return false;
        }
        if (nVal == null) {
            if (other.nVal != null) {
                return false;
            }
        } else if (!nVal.equals(other.nVal)) {
            return false;
        }
        if (oldVClock == null) {
            if (other.oldVClock != null) {
                return false;
            }
        } else if (!oldVClock.equals(other.oldVClock)) {
            return false;
        }
        if (postcommitHooks == null) {
            if (other.postcommitHooks != null) {
                return false;
            }
        } else if (!postcommitHooks.equals(other.postcommitHooks)) {
            return false;
        }
        if (precommitHooks == null) {
            if (other.precommitHooks != null) {
                return false;
            }
        } else if (!precommitHooks.equals(other.precommitHooks)) {
            return false;
        }
        if (r == null) {
            if (other.r != null) {
                return false;
            }
        } else if (!r.equals(other.r)) {
            return false;
        }
        if (rw == null) {
            if (other.rw != null) {
                return false;
            }
        } else if (!rw.equals(other.rw)) {
            return false;
        }
        if (smallVClock == null) {
            if (other.smallVClock != null) {
                return false;
            }
        } else if (!smallVClock.equals(other.smallVClock)) {
            return false;
        }
        if (w == null) {
            if (other.w != null) {
                return false;
            }
        } else if (!w.equals(other.w)) {
            return false;
        }
        if (youngVClock == null) {
            if (other.youngVClock != null) {
                return false;
            }
        } else if (!youngVClock.equals(other.youngVClock)) {
            return false;
        }
        return true;
    }
}
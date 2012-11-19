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

import javax.annotation.concurrent.Immutable;

/**
 * Parameter wrapper class for the set of vclock prune properties
 * 
 * @author russell
 * @see BucketProperties
 * 
 */
@Immutable
public class VClockPruneProps {

    private final Integer smallVClock;
    private final Integer bigVClock;
    private final Long youngVClock;
    private final Long oldVClock;

    /**
     * Create an instance for the provided set of prone properties
     * 
     * @param smallVClock
     * @param bigVClock
     * @param youngVClock
     * @param oldVClock
     */
    public VClockPruneProps(Integer smallVClock, Integer bigVClock, Long youngVClock, Long oldVClock) {
        this.smallVClock = smallVClock;
        this.bigVClock = bigVClock;
        this.youngVClock = youngVClock;
        this.oldVClock = oldVClock;
    }

    /**
     * the small_vclock property for this bucket. See <a href=
     * "https://help.basho.com/entries/448442-how-does-riak-control-the-growth-of-vector-clocks"
     * >controlling vector clock growth</a> for details.
     * 
     * @return the small vector clock pruning property if set, or null.
     */
    public Integer getSmallVClock() {
        return smallVClock;
    }

    /**
     * the big_vclock property for this bucket. See <a href=
     * "https://help.basho.com/entries/448442-how-does-riak-control-the-growth-of-vector-clocks"
     * >controlling vector clock growth</a> for details.
     * 
     * @return the big vclock pruning size property if set, or null.
     */
    public Integer getBigVClock() {
        return bigVClock;
    }

    /**
     * The young_vclcok property for this bucket. See <a href=
     * "https://help.basho.com/entries/448442-how-does-riak-control-the-growth-of-vector-clocks"
     * >controlling vector clock growth</a> for details.
     * 
     * @return the young vclock prune property if set, or null.
     */
    public Long getYoungVClock() {
        return youngVClock;
    }

    /**
     * the old_vclock property for this bucket. See <a href=
     * "https://help.basho.com/entries/448442-how-does-riak-control-the-growth-of-vector-clocks"
     * >controlling vector clock growth</a> for details.
     * 
     * @return the old vclock prune property if set, or null
     */
    public Long getOldVClock() {
        return oldVClock;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((bigVClock == null) ? 0 : bigVClock.hashCode());
        result = prime * result + ((oldVClock == null) ? 0 : oldVClock.hashCode());
        result = prime * result + ((smallVClock == null) ? 0 : smallVClock.hashCode());
        result = prime * result + ((youngVClock == null) ? 0 : youngVClock.hashCode());
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
        if (!(obj instanceof VClockPruneProps)) {
            return false;
        }
        VClockPruneProps other = (VClockPruneProps) obj;
        if (bigVClock == null) {
            if (other.bigVClock != null) {
                return false;
            }
        } else if (!bigVClock.equals(other.bigVClock)) {
            return false;
        }
        if (oldVClock == null) {
            if (other.oldVClock != null) {
                return false;
            }
        } else if (!oldVClock.equals(other.oldVClock)) {
            return false;
        }
        if (smallVClock == null) {
            if (other.smallVClock != null) {
                return false;
            }
        } else if (!smallVClock.equals(other.smallVClock)) {
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

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override public String toString() {
        return String.format("VClockPruneProps [smallVClock=%s, bigVClock=%s, youngVClock=%s, oldVClock=%s]",
                             smallVClock, bigVClock, youngVClock, oldVClock);
    }
}

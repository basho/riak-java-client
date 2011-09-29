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

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.builders.BucketPropertiesBuilder;
import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.query.functions.NamedErlangFunction;
import com.basho.riak.client.query.functions.NamedFunction;

/**
 * An immutable implementation of {@link BucketProperties}.
 * 
 * <p>
 * Use {@link BucketPropertiesBuilder} if you really have to make one, but your
 * better to fetch one like with {@link IRiakClient#fetchBucket(String)} or create one with
 * {@link IRiakClient#createBucket(String)}
 * </p>
 * @author russell
 * 
 * @see IRiakClient
 * @see WriteBucket
 * @see FetchBucket
 */
public class DefaultBucketProperties implements BucketProperties {

    private final Boolean allowSiblings;
    private final Boolean lastWriteWins;
    private final Integer nVal;
    private final String backend;
    private final VClockPruneProps vclockProps;
    private final Collection<NamedFunction> precommitHooks;
    private final Collection<NamedErlangFunction> postcommitHooks;
    private final TunableCAPProps capProps;
    private final NamedErlangFunction chashKeyFunction;
    private final NamedErlangFunction linkWalkFunction;
    private final Boolean search;

    /**
     * Use the Builder {@link BucketPropertiesBuilder} instead of calling this constructor directly
     * 
     * @param allowSiblings
     * @param lastWriteWins
     * @param nVal
     * @param backend
     * @param vclockProps
     * @param precommitHooks
     * @param postcommitHooks
     * @param capProps
     * @param chashKeyFunction
     * @param linkWalkFunction
     * @param search
     */
    public DefaultBucketProperties(Boolean allowSiblings, Boolean lastWriteWins, Integer nVal, String backend,
            VClockPruneProps vclockProps,
            Collection<NamedFunction> precommitHooks, Collection<NamedErlangFunction> postcommitHooks,
            TunableCAPProps capProps, NamedErlangFunction chashKeyFunction, NamedErlangFunction linkWalkFunction, Boolean search) {
        this.allowSiblings = allowSiblings;
        this.lastWriteWins = lastWriteWins;
        this.nVal = nVal;
        this.backend = backend;
        this.vclockProps = vclockProps;
        this.precommitHooks = precommitHooks;
        this.postcommitHooks = postcommitHooks;
        this.capProps = capProps;
        this.chashKeyFunction = chashKeyFunction;
        this.linkWalkFunction = linkWalkFunction;
        this.search = search;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.bucket.BucketProperties#getAllowSiblings()
     */
    public Boolean getAllowSiblings() {
        return allowSiblings;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.bucket.BucketProperties#getLastWriteWins()
     */
    public Boolean getLastWriteWins() {
        return lastWriteWins;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.bucket.BucketProperties#getNVal()
     */
    public Integer getNVal() {
        return nVal;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.bucket.BucketProperties#getBackend()
     */
    public String getBackend() {
        return backend;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.bucket.BucketProperties#getSmallVClock()
     */
    public Integer getSmallVClock() {
        return vclockProps.getSmallVClock();
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.bucket.BucketProperties#getBigVClock()
     */
    public Integer getBigVClock() {
        return vclockProps.getBigVClock();
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.bucket.BucketProperties#getYoungVClock()
     */
    public Long getYoungVClock() {
        return vclockProps.getYoungVClock();
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.bucket.BucketProperties#getOldVClock()
     */
    public Long getOldVClock() {
        return vclockProps.getOldVClock();
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.bucket.BucketProperties#getPrecommitHooks()
     */
    public Collection<NamedFunction> getPrecommitHooks() {
        return precommitHooks;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.bucket.BucketProperties#getPostcommitHooks()
     */
    public Collection<NamedErlangFunction> getPostcommitHooks() {
        return postcommitHooks;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.bucket.BucketProperties#getR()
     */
    public Quorum getR() {
        return capProps.getR();
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.bucket.BucketProperties#getW()
     */
    public Quorum getW() {
        return capProps.getW();
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.bucket.BucketProperties#getRW()
     */
    public Quorum getRW() {
        return capProps.getRW();
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.bucket.BucketProperties#getDW()
     */
    public Quorum getDW() {
        return capProps.getDW();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.bucket.BucketProperties#getPR()
     */
    public Quorum getPR() {
        return capProps.getPR();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.bucket.BucketProperties#getPW()
     */
    public Quorum getPW() {
        return capProps.getPW();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.bucket.BucketProperties#getBasicQuorum()
     */
    public Boolean getBasicQuorum() {
        return capProps.getBasicQuorum();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.bucket.BucketProperties#getNotFoundOK()
     */
    public Boolean getNotFoundOK() {
        return capProps.getNotFoundOK();
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.bucket.BucketProperties#getChashKeyFunction()
     */
    public NamedErlangFunction getChashKeyFunction() {
        return chashKeyFunction;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.bucket.BucketProperties#getLinkWalkFunction()
     */
    public NamedErlangFunction getLinkWalkFunction() {
        return linkWalkFunction;
    }

    /**
     * Create a {@link BucketPropertiesBuilder} populated with my values.
     * @return a Builder populated from this BucketProperties' values.
     */
    public BucketPropertiesBuilder fromMe() {
        return DefaultBucketProperties.from(this);
    }

    /**
     * Create a {@link BucketPropertiesBuilder} populated from the given
     * {@link DefaultBucketProperties}.
     * 
     * @param properties
     * @return a Builder populated with properties values.
     */
    public static BucketPropertiesBuilder from(DefaultBucketProperties properties) {
        return BucketPropertiesBuilder.from(properties);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.bucket.BucketProperties#getSearch()
     */
    public Boolean getSearch() {
        return this.search;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.bucket.BucketProperties#isSearchEnabled()
     */
    public boolean isSearchEnabled() {
        if (Boolean.TRUE.equals(search) || precommitHooks.contains(NamedErlangFunction.SEARCH_PRECOMMIT_HOOK)) {
            return true;
        }
        return false;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((allowSiblings == null) ? 0 : allowSiblings.hashCode());
        result = prime * result + ((backend == null) ? 0 : backend.hashCode());
        result = prime * result + ((capProps == null) ? 0 : capProps.hashCode());
        result = prime * result + ((chashKeyFunction == null) ? 0 : chashKeyFunction.hashCode());
        result = prime * result + ((lastWriteWins == null) ? 0 : lastWriteWins.hashCode());
        result = prime * result + ((linkWalkFunction == null) ? 0 : linkWalkFunction.hashCode());
        result = prime * result + ((nVal == null) ? 0 : nVal.hashCode());
        result = prime * result + ((postcommitHooks == null) ? 0 : postcommitHooks.hashCode());
        result = prime * result + ((precommitHooks == null) ? 0 : precommitHooks.hashCode());
        result = prime * result + ((search == null) ? 0 : search.hashCode());
        result = prime * result + ((vclockProps == null) ? 0 : vclockProps.hashCode());
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
        if (capProps == null) {
            if (other.capProps != null) {
                return false;
            }
        } else if (!capProps.equals(other.capProps)) {
            return false;
        }
        if (chashKeyFunction == null) {
            if (other.chashKeyFunction != null) {
                return false;
            }
        } else if (!chashKeyFunction.equals(other.chashKeyFunction)) {
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
        if (search == null) {
            if (other.search != null) {
                return false;
            }
        } else if (!search.equals(other.search)) {
            return false;
        }
        if (vclockProps == null) {
            if (other.vclockProps != null) {
                return false;
            }
        } else if (!vclockProps.equals(other.vclockProps)) {
            return false;
        }
        return true;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override public String toString() {
        return String.format("DefaultBucketProperties [allowSiblings=%s, lastWriteWins=%s, nVal=%s, backend=%s, vclockProps=%s, precommitHooks=%s, postcommitHooks=%s, capProps=%s, chashKeyFunction=%s, linkWalkFunction=%s, search=%s]",
                             allowSiblings, lastWriteWins, nVal, backend, vclockProps, precommitHooks, postcommitHooks,
                             capProps, chashKeyFunction, linkWalkFunction, search);
    }
}
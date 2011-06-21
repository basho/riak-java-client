package com.basho.riak.pbc;

import com.basho.riak.pbc.RPB.RpbBucketProps;
import com.basho.riak.pbc.RPB.RpbGetBucketResp;
import com.basho.riak.pbc.RPB.RpbBucketProps.Builder;

/**
 * PBC's limited model of Riak Bucket properties.
 */
public class BucketProperties {

	private Boolean allowMult;
	private Integer nValue;

	public void init(RpbGetBucketResp resp) {
		if (resp.hasProps()) {
			
			RpbBucketProps props = resp.getProps();
			if (props.hasAllowMult()) {
				allowMult = Boolean.valueOf(props.getAllowMult());
			}
			if (props.hasNVal()) {
				nValue = new Integer(props.getNVal());
			}
		}
	}
	
	public Boolean getAllowMult() {
		return allowMult;
	}
	
	public Integer getNValue() {
		return nValue;
	}
	
	public BucketProperties allowMult(boolean val) {
		this.allowMult = val;
		return this;
	}

	public BucketProperties nValue(int val) {
		this.nValue = val;
		return this;
	}

	RpbBucketProps build() {
		Builder builder = RpbBucketProps.newBuilder();
		if (allowMult != null) {
			builder.setAllowMult(allowMult);
		}
		if (nValue != null) {
			builder.setNVal(nValue);
		}
		return builder.build();
	}

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((allowMult == null) ? 0 : allowMult.hashCode());
        result = prime * result + ((nValue == null) ? 0 : nValue.hashCode());
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
        if (!(obj instanceof BucketProperties)) {
            return false;
        }
        BucketProperties other = (BucketProperties) obj;
        if (allowMult == null) {
            if (other.allowMult != null) {
                return false;
            }
        } else if (!allowMult.equals(other.allowMult)) {
            return false;
        }
        if (nValue == null) {
            if (other.nValue != null) {
                return false;
            }
        } else if (!nValue.equals(other.nValue)) {
            return false;
        }
        return true;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override public String toString() {
        return String.format("BucketProperties [allowMult=%s, nValue=%s]", allowMult, nValue);
    }

}

package com.basho.riak.pbc;

import com.basho.riak.pbc.RPB.RpbBucketProps;
import com.basho.riak.pbc.RPB.RpbGetBucketResp;
import com.basho.riak.pbc.RPB.RpbBucketProps.Builder;

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

}

package com.basho.riak.pbc;

import com.basho.riak.protobuf.RiakPB.RpbBucketProps;
import com.basho.riak.protobuf.RiakPB.RpbBucketProps.Builder;
import com.basho.riak.protobuf.RiakPB.RpbCommitHook;
import com.basho.riak.protobuf.RiakPB.RpbGetBucketResp;
import com.basho.riak.protobuf.RiakPB.RpbModFun;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;

/**
 * PBC's model of Riak Bucket properties.
 */
public class BucketProperties {

	private Boolean allowMult;
	private Integer nValue;
    private Boolean lastWriteWins;
    private List<CommitHook> precommitHooks = new ArrayList<CommitHook>();
    private List<CommitHook> postcommitHooks = new ArrayList<CommitHook>();
    private ModuleFunction cHashFun;
    private ModuleFunction linkFun;
    private Long oldVClock;
    private Long youngVClock;
    private Integer bigVClock;
    private Integer smallVClock;
    private Integer pr;
    private Integer r;
    private Integer w;
    private Integer pw;
    private Integer dw;
    private Integer rw;
    private Boolean basicQuorum;
    private Boolean notFoundOk;
    private Boolean searchEnabled;
    private String backend;
    

	public void init(RpbGetBucketResp resp) {
		if (resp.hasProps()) {
			
			RpbBucketProps props = resp.getProps();
			if (props.hasAllowMult()) {
				allowMult = Boolean.valueOf(props.getAllowMult());
			}
			if (props.hasNVal()) {
				nValue = new Integer(props.getNVal());
			}
            
            if (props.hasLastWriteWins()) {
                lastWriteWins = props.getLastWriteWins();
            }
            
            if (props.hasHasPrecommit()) {
                precommitHooks = parseHooks(props.getPrecommitList());
            }
            
            if (props.hasHasPostcommit()) {
                postcommitHooks = parseHooks(props.getPostcommitList());
            }
            
            if (props.hasChashKeyfun()) {
                cHashFun = new ModuleFunction(props.getChashKeyfun().getModule().toStringUtf8(),
                                               props.getChashKeyfun().getFunction().toStringUtf8());
            }
            
            if (props.hasLinkfun()) {
                linkFun = new ModuleFunction(props.getLinkfun().getModule().toStringUtf8(),
                                               props.getLinkfun().getFunction().toStringUtf8());
            }
            
            if (props.hasPr()) {
                pr = props.getPr();
            }
            
            if (props.hasR()) {
                r = props.getR();
            }
            
            if (props.hasW()) {
                w = props.getW();
            }
            
            if (props.hasPw()) {
                pw = props.getPw();
            }
            
            if (props.hasDw()) {
                dw = props.getDw();
            }
            
            if (props.hasRw()) {
                rw = props.getRw();
            }
            
            if (props.hasBasicQuorum()) {
                basicQuorum = props.getBasicQuorum();
            }
            
            if (props.hasNotfoundOk()) {
                notFoundOk = props.getNotfoundOk();
            }
            
            if (props.hasSearch()) {
                searchEnabled = props.getSearch();
            }
            
            if (props.hasBackend()) {
                backend = props.getBackend().toStringUtf8();
            }
            
            if (props.hasYoungVclock()) {
                youngVClock = getUnsignedIntValue(props.getYoungVclock());
            }
            
            if (props.hasOldVclock()) {
                oldVClock = getUnsignedIntValue(props.getOldVclock());
            }
            
            if (props.hasBigVclock()) {
                bigVClock = props.getBigVclock();
            }
            
            if (props.hasSmallVclock()) {
                smallVClock = props.getSmallVclock();
            }
            
		}
	}
	
    private long getUnsignedIntValue(int i) {
        return i & 0x00000000ffffffffL;
    }
    
    private List<CommitHook> parseHooks(List<RpbCommitHook> hooks) {
        List<CommitHook> list = new ArrayList<CommitHook>(hooks.size());
        for ( RpbCommitHook hook : hooks) {
            if (hook.hasName()) {
                CommitHook ph = new CommitHook(hook.getName().toStringUtf8());
                list.add(ph);
            } else {
                CommitHook ph = new CommitHook(hook.getModfun().getModule().toStringUtf8(),
                                                hook.getModfun().getFunction().toStringUtf8());
                list.add(ph);
            }
        }
        return list;
    }
    
    private List<RpbCommitHook> convertHooks(List<CommitHook> hookList) {
        List<RpbCommitHook> pbHookList = new ArrayList<RpbCommitHook>(hookList.size());
        RpbCommitHook.Builder builder = RpbCommitHook.newBuilder();
        RpbModFun.Builder mfBuilder = RpbModFun.newBuilder();
        for (CommitHook hook : hookList) {
            if (hook.isJavascript()) {
                builder.setName(ByteString.copyFromUtf8(hook.getJsName()));
            } else {
                mfBuilder.setModule(ByteString.copyFromUtf8(hook.getErlModule()));
                mfBuilder.setFunction(ByteString.copyFromUtf8(hook.getErlFunction()));
                builder.setModfun(mfBuilder);
            }
            
            pbHookList.add(builder.build());
            builder.clear();
            mfBuilder.clear();
        }
        return pbHookList;
    }
    
    private RpbModFun convertModFun(ModuleFunction mf) {
        return RpbModFun.newBuilder()
                .setModule(ByteString.copyFromUtf8(mf.getModule()))
                .setFunction(ByteString.copyFromUtf8(mf.getFunction()))
                .build();
        
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
        
        if (lastWriteWins != null) {
            builder.setLastWriteWins(lastWriteWins);
        }
        
        if (backend != null) {
            builder.setBackend(ByteString.copyFromUtf8(backend));
        }
        
        if (smallVClock != null) {
            builder.setSmallVclock(smallVClock);
        }
        
        if (bigVClock != null) {
            builder.setBigVclock(bigVClock);
        }
        
        if (youngVClock != null) {
            builder.setYoungVclock(youngVClock.intValue());
        }
        
        if (oldVClock != null) {
            builder.setOldVclock(oldVClock.intValue());
        }
        
        if (r != null) {
            builder.setR(r);
        }
        
        if (w != null) {
            builder.setW(w);
        }
        
        if (rw != null) {
            builder.setRw(rw);
        }
        
        if (dw != null) { 
            builder.setDw(dw);
        }
        
        if (pr != null) {
            builder.setPr(pr);
        }
        
        if (pw != null) {
            builder.setPw(pw);
        }
        
        if (basicQuorum != null) {
            builder.setBasicQuorum(basicQuorum);
        }
        
        if (notFoundOk != null) {
            builder.setNotfoundOk(notFoundOk);
        }
        
        if (searchEnabled != null) {
            builder.setSearch(searchEnabled);
        }
        
        if (precommitHooks.size() > 0) {
            builder.addAllPrecommit(convertHooks(precommitHooks));
        }
        
        if (postcommitHooks.size() > 0) {
            builder.addAllPostcommit(convertHooks(postcommitHooks));
        }
        
        if (cHashFun != null) {
            builder.setChashKeyfun(convertModFun(cHashFun));
        }
        
        if (linkFun != null) {
            builder.setLinkfun(convertModFun(linkFun));
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

    /**
     * @return the lastWriteWins
     */
    public Boolean getLastWriteWins() {
        return lastWriteWins;
    }

    /**
     * @param lastWriteWins the lastWriteWins to set
     */
    public BucketProperties lastWriteWins(Boolean lastWriteWins) {
        this.lastWriteWins = lastWriteWins;
        return this;
    }

    /**
     * @return the precommitHooks
     */
    public List<CommitHook> getPrecommitHooks() {
        return precommitHooks;
    }

    /**
     * @param precommitHooks the precommitHooks to set
     */
    public BucketProperties precommitHooks(List<CommitHook> precommitHooks) {
        this.precommitHooks = precommitHooks;
        return this;
    }

    /**
     * @return the postcommitHooks
     */
    public List<CommitHook> getPostcommitHooks() {
        return postcommitHooks;
    }

    /**
     * @param postcommitHooks the postcommitHooks to set
     */
    public BucketProperties postcommitHooks(List<CommitHook> postcommitHooks) {
        this.postcommitHooks = postcommitHooks;
        return this;
    }

    /**
     * @return the cHashFun
     */
    public ModuleFunction getcHashFun() {
        return cHashFun;
    }

    /**
     * @param cHashFun the cHashFun to set
     */
    public BucketProperties cHashFun(ModuleFunction cHashFun) {
        this.cHashFun = cHashFun;
        return this;
    }

    /**
     * @return the linkFun
     */
    public ModuleFunction getLinkFun() {
        return linkFun;
    }

    /**
     * @param linkFun the linkFun to set
     */
    public BucketProperties linkFun(ModuleFunction linkFun) {
        this.linkFun = linkFun;
        return this;
    }

    /**
     * @return the pr
     */
    public Integer getPr() {
        return pr;
    }

    /**
     * @param pr the pr to set
     */
    public BucketProperties pr(Integer pr) {
        this.pr = pr;
        return this;
    }

    /**
     * @return the r
     */
    public Integer getR() {
        return r;
    }

    /**
     * @param r the r to set
     */
    public BucketProperties r(Integer r) {
        this.r = r;
        return this;
    }

    /**
     * @return the w
     */
    public Integer getW() {
        return w;
    }

    /**
     * @param w the w to set
     */
    public BucketProperties w(Integer w) {
        this.w = w;
        return this;
    }

    /**
     * @return the pw
     */
    public Integer getPw() {
        return pw;
    }

    /**
     * @param pw the pw to set
     */
    public BucketProperties pw(Integer pw) {
        this.pw = pw;
        return this;
    }

    /**
     * @return the dw
     */
    public Integer getDw() {
        return dw;
    }

    /**
     * @param dw the dw to set
     */
    public BucketProperties dw(Integer dw) {
        this.dw = dw;
        return this;
    }

    /**
     * @return the rw
     */
    public Integer getRw() {
        return rw;
    }

    /**
     * @param rw the rw to set
     */
    public BucketProperties rw(Integer rw) {
        this.rw = rw;
        return this;
    }

    /**
     * @return the basicQuorum
     */
    public Boolean getBasicQuorum() {
        return basicQuorum;
    }

    /**
     * @param basicQuorum the basicQuorum to set
     */
    public BucketProperties basicQuorum(Boolean basicQuorum) {
        this.basicQuorum = basicQuorum;
        return this;
    }

    /**
     * @return the notFoundOk
     */
    public Boolean getNotFoundOk() {
        return notFoundOk;
    }

    /**
     * @param notFoundOk the notFoundOk to set
     */
    public BucketProperties notFoundOk(Boolean notFoundOk) {
        this.notFoundOk = notFoundOk;
        return this;
    }
    
    /**
     * @return the searchEnabled
     */
    public Boolean getSearchEnabled() {
        return searchEnabled;
    }
    
    /**
     * * @param searchEnabled 
     */
    public BucketProperties searchEnabled(Boolean searchEnabled) {
        this.searchEnabled = searchEnabled;
        return this;
    }

    /**
     * @return the backend
     */
    public String getBackend() {
        return backend;
    }
    
    /**
     * @param backend 
     */
    public BucketProperties backend(String backend) {
        this.backend = backend;
        return this;
    }
    
    public Long getYoungVClock() {
        return youngVClock;
    }
    
    public BucketProperties youngVClock(Long youngVClock) {
        this.youngVClock = youngVClock;
        return this;
    }
    
    public Long getOldVClock() {
        return oldVClock;
    }
    
    public BucketProperties oldVclock(Long oldVClock) {
        this.oldVClock = oldVClock;
        return this;
    }
    
    public Integer getSmallVClock() {
        return smallVClock;
    }
    
    public BucketProperties smallVClock(Integer smallVClock) {
        this.smallVClock = smallVClock;
        return this;
    }
    
    public Integer getBigVClock() {
        return bigVClock;
    }
    
    public BucketProperties bigVClock(Integer bigVClock) {
        this.bigVClock = bigVClock;
        return this;
    }
}

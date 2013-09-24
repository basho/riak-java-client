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
package com.basho.riak.client;

import com.basho.riak.client.cap.Quora;
import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.cap.VClock;

/**
 * The set of parameters for a delete operation
 *
 * @author russell
 */
public class DeleteMeta
{

	private final Quorum r;
	private final Quorum pr;
	private final Quorum w;
	private final Quorum dw;
	private final Quorum pw;
	private final Quorum rw;
	private final VClock vclock;
	private final Integer timeout;
	private final Integer nval;
	private final Boolean sloppyQuorum;

	/**
	 * Any of the parameters may be null.
	 *
	 * @param r      how many vnodes must reply
	 * @param pr     how many primary vnodes must reply, takes precedence over r
	 * @param w      the write quorum for a store operation
	 * @param dw     the durable write quorum for a store operation
	 * @param pw     the primary write quorum
	 * @param rw     how many replicas must reply before considered deleted
	 * @param vclock the last seen vclock value before this delete operation
	 */
	public DeleteMeta(Integer r, Integer pr, Integer w, Integer dw, Integer pw, Integer rw, VClock vclock, Integer timeout,
										Integer nval, Boolean sloppyQuorum)
	{
		this(null == r ? null : new Quorum(r),
			null == pr ? null : new Quorum(pr),
			null == w ? null : new Quorum(w),
			null == dw ? null : new Quorum(dw),
			null == pw ? null : new Quorum(pw),
			null == rw ? null : new Quorum(rw),
			vclock,
			timeout,
			nval,
			sloppyQuorum
		);

	}

	/**
	 * Any of the parameters may be null.
	 *
	 * @param r      how many vnodes must reply
	 * @param pr     how many primary vnodes must reply, takes precedence over r
	 * @param w      the write quorum for a store operation
	 * @param dw     the durable write quorum for a store operation
	 * @param pw     the primary write quorum
	 * @param rw     how many replicas must reply before considered deleted
	 * @param vclock the last seen vclock value before this delete operation
	 */
	public DeleteMeta(Quorum r, Quorum pr, Quorum w, Quorum dw, Quorum pw, Quorum rw, VClock vclock, Integer timeout,
										Integer nval, Boolean sloppyQuorum)
	{
		this.r = r;
		this.pr = pr;
		this.w = w;
		this.dw = dw;
		this.pw = pw;
		this.rw = rw;
		this.vclock = vclock;
		this.timeout = timeout;
		this.nval = nval;
		this.sloppyQuorum = sloppyQuorum;
	}

	/**
	 * @return true is the r parameter is set, false otherwise.
	 */
	public boolean hasR()
	{
		return r != null;
	}

	/**
	 * @return r parameter or null
	 */
	public Quorum getR()
	{
		return r;
	}

	/**
	 * @return true if the pr parameter is set, false otherwise
	 */
	public boolean hasPr()
	{
		return pr != null;
	}

	/**
	 * @return the pr parameter, or null
	 */
	public Quorum getPr()
	{
		return pr;
	}

	/**
	 * @return true if the w parameter is set, false otherwise
	 */
	public boolean hasW()
	{
		return w != null;
	}

	/**
	 * @return the w parameter or null
	 */
	public Quorum getW()
	{
		return w;
	}

	/**
	 * @return true if the dw parameter is set, false otherwise
	 */
	public boolean hasDw()
	{
		return dw != null;
	}

	/**
	 * @return the dw parameter, or null
	 */
	public Quorum getDw()
	{
		return dw;
	}

	/**
	 * @return true is the pw parameter is set, false otherwise.
	 */
	public boolean hasPw()
	{
		return pw != null;
	}

	/**
	 * @return pw parameter, or null
	 */
	public Quorum getPw()
	{
		return pw;
	}

	/**
	 * @return true if the rw parameter is set
	 */
	public boolean hasRw()
	{
		return rw != null;
	}

	/**
	 * @return the rw, or null if not set.
	 */
	public Quorum getRw()
	{
		return rw;
	}

	/**
	 * @return true if this delete meta has a vclock
	 */
	public boolean hasVclock()
	{
		return vclock != null;
	}

	/**
	 * @return the vclock or null if not set
	 */
	public VClock getVclock()
	{
		return vclock;
	}

	/**
	 * @return true if the timeout parameter is set, false otherwise
	 */
	public boolean hasTimeout()
	{
		return timeout != null;
	}

	/**
	 * @return the timeout in milliseconds if set, null otherwise
	 */
	public Integer getTimeout()
	{
		return timeout;
	}

	// Builder
	public static class Builder
	{
		private Quorum r;
		private Quorum pr;
		private Quorum w;
		private Quorum dw;
		private Quorum pw;
		private Quorum rw;
		private VClock vclock;
		private Integer timeout;
		private Integer nval;
		private Boolean sloppyQuorum;

		public DeleteMeta build()
		{
			return new DeleteMeta(r, pr, w, dw, pw, rw, vclock, timeout, nval, sloppyQuorum);
		}

		public Builder r(int r)
		{
			this.r = new Quorum(r);
			return this;
		}

		public Builder r(Quora r)
		{
			this.r = new Quorum(r);
			return this;
		}

		public Builder r(Quorum r)
		{
			this.r = r;
			return this;
		}

		public Builder pr(int pr)
		{
			this.pr = new Quorum(pr);
			return this;
		}

		public Builder pr(Quora pr)
		{
			this.pr = new Quorum(pr);
			return this;
		}

		public Builder pr(Quorum pr)
		{
			this.pr = pr;
			return this;
		}

		public Builder w(int w)
		{
			this.w = new Quorum(w);
			return this;
		}

		public Builder w(Quora w)
		{
			this.w = new Quorum(w);
			return this;
		}

		public Builder w(Quorum w)
		{
			this.w = w;
			return this;
		}

		public Builder dw(int dw)
		{
			this.dw = new Quorum(dw);
			return this;
		}

		public Builder dw(Quora dw)
		{
			this.dw = new Quorum(dw);
			return this;
		}

		public Builder dw(Quorum dw)
		{
			this.dw = dw;
			return this;
		}

		public Builder pw(int pw)
		{
			this.pw = new Quorum(pw);
			return this;
		}

		public Builder pw(Quora pw)
		{
			this.pw = new Quorum(pw);
			return this;
		}

		public Builder pw(Quorum pw)
		{
			this.pw = pw;
			return this;
		}

		public Builder rw(int rw)
		{
			this.rw = new Quorum(rw);
			return this;
		}

		public Builder rw(Quora rw)
		{
			this.rw = new Quorum(rw);
			return this;
		}

		public Builder rw(Quorum rw)
		{
			this.rw = rw;
			return this;
		}

		public Builder vclock(VClock vclock)
		{
			this.vclock = vclock;
			return this;
		}

		public Builder timeout(Integer timeout)
		{
			this.timeout = timeout;
			return this;
		}

		public Builder nval(Integer nval)
		{
			this.nval = nval;
			return this;
		}

		public Builder sloppyQuorum(Boolean sloppyQuorum)
		{
			this.sloppyQuorum = sloppyQuorum;
			return this;
		}
	}
}
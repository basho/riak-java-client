package com.basho.riak.client.operations;

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.operations.StoreBucketPropsOperation;
import com.basho.riak.client.query.functions.Function;

import java.util.concurrent.ExecutionException;

public class StoreBucketProperties extends RiakCommand<Boolean>
{

	private final Location location;
	private final Boolean allowMulti;
	private final String backend;
	private final Boolean basicQuorum;
	private final Long bigVClock;
	private final Function chashkeyFunction;
	private final Boolean lastWriteWins;
	private final Function linkWalkFunction;
	private final Integer rw;
	private final Integer dw;
	private final Integer w;
	private final Integer r;
	private final Integer pr;
	private final Integer pw;
	private final Boolean notFoundOk;
	private final Function preCommitHook;
	private final Function postCommitHook;
	private final Long oldVClock;
	private final Long youngVClock;
	private final Long smallVClock;
	private final Integer nval;
	private final Boolean legacySearch;
	private final String searchIndex;

	StoreBucketProperties(Builder builder)
	{

		this.location = builder.location;
		this.allowMulti = builder.allowMulti;
		this.backend = builder.backend;
		this.bigVClock = builder.bigVClock;
		this.chashkeyFunction = builder.chashkeyFunction;
		this.lastWriteWins = builder.lastWriteWins;
		this.basicQuorum = builder.basicQuorum;
		this.linkWalkFunction = builder.linkWalkFunction;
		this.rw = builder.rw;
		this.dw = builder.dw;
		this.w = builder.w;
		this.pr = builder.pr;
		this.pw = builder.pw;
		this.r = builder.r;
		this.notFoundOk = builder.notFoundOk;
		this.preCommitHook = builder.preCommitHook;
		this.postCommitHook = builder.postCommitHook;
		this.oldVClock = builder.oldVClock;
		this.youngVClock = builder.youngVClock;
		this.smallVClock = builder.smallVClock;
		this.nval = builder.nval;
		this.legacySearch = builder.legacySearch;
		this.searchIndex = builder.searchIndex;

	}

	@Override
	public Boolean execute(RiakCluster cluster) throws ExecutionException, InterruptedException
	{

		StoreBucketPropsOperation.Builder builder = new StoreBucketPropsOperation.Builder(location.getBucket());

		if (location.hasType())
		{
			builder.withBucketType(location.getType());
		}

		if (allowMulti != null)
		{
			builder.withAllowMulti(allowMulti);
		}

		if (backend != null)
		{
			builder.withBackend(backend);
		}

		if (basicQuorum != null)
		{
			builder.withBasicQuorum(basicQuorum);
		}

		if (bigVClock != null)
		{
			builder.withBigVClock(bigVClock);
		}

		if (chashkeyFunction != null)
		{
			builder.withChashkeyFunction(chashkeyFunction);
		}

		if (lastWriteWins != null)
		{
			builder.withLastWriteWins(lastWriteWins);
		}

		if (linkWalkFunction != null)
		{
			builder.withLinkwalkFunction(linkWalkFunction);
		}

		if (rw != null)
		{
			builder.withRw(rw);
		}

		if (dw != null)
		{
			builder.withDw(dw);
		}

		if (w != null)
		{
			builder.withW(w);
		}

		if (r != null)
		{
			builder.withR(r);
		}

		if (pr != null)
		{
			builder.withPr(pr);
		}

		if (pw != null)
		{
			builder.withPw(pw);
		}

		if (notFoundOk != null)
		{
			builder.withNotFoundOk(notFoundOk);
		}

		if (preCommitHook != null)
		{
			builder.withPrecommitHook(preCommitHook);
		}

		if (postCommitHook != null)
		{
			builder.withPostcommitHook(postCommitHook);
		}

		if (oldVClock != null)
		{
			builder.withOldVClock(oldVClock);
		}

		if (youngVClock != null)
		{
			builder.withYoungVClock(youngVClock);
		}

		if (smallVClock != null)
		{
			builder.withSmallVClock(smallVClock);
		}

		if (nval != null)
		{
			builder.withNVal(nval);
		}

		if (legacySearch != null)
		{
			builder.withLegacyRiakSearchEnabled(legacySearch);
		}

		if (searchIndex != null)
		{
			builder.withSearchIndex(searchIndex);
		}

		return cluster.execute(builder.build()).get();

	}

	public static class Builder
	{

		private final Location location;
		private Boolean allowMulti;
		private String backend;
		private Boolean basicQuorum;
		private Long bigVClock;
		private Function chashkeyFunction;
		private Boolean lastWriteWins;
		private Function linkWalkFunction;
		private Integer rw;
		private Integer dw;
		private Integer w;
		private Integer r;
		private Integer pr;
		private Integer pw;
		private Boolean notFoundOk;
		private Function preCommitHook;
		private Function postCommitHook;
		private Long oldVClock;
		private Long youngVClock;
		private Long smallVClock;
		private Integer nval;
		private Boolean legacySearch;
		private String searchIndex;


		public Builder(Location location)
		{
			this.location = location;
		}

		/**
		 * Set the allow_multi value.
		 *
		 * @param allow whether to allow sibling objects to be created.
		 * @return a reference to this object.
		 */
		public Builder withAllowMulti(boolean allow)
		{
			this.allowMulti = allow;
			return this;
		}

		/**
		 * Set the backend used by this bucket. Only applies when using
		 * {@code riak_kv_multi_backend} in Riak.
		 *
		 * @param backend the name of the backend to use.
		 * @return a reference to this object.
		 */
		public Builder withBackend(String backend)
		{
			if (null == backend || backend.length() == 0)
			{
				throw new IllegalArgumentException("Backend can not be null or zero length");
			}
			this.backend = backend;
			return this;
		}

		/**
		 * Set the basic_quorum value.
		 *
		 * The parameter controls whether a read request should return early in
		 * some fail cases. E.g. If a quorum of nodes has already returned
		 * notfound/error, don't wait around for the rest.
		 *
		 * @param use the basic_quorum value.
		 * @return a reference to this object.
		 */
		public Builder withBasicQuorum(boolean use)
		{
			this.basicQuorum = use;
			return this;
		}

		/**
		 * Set the big_vclock value.
		 *
		 * @param bigVClock a long representing a epoch time value.
		 * @return a reference to this object.
		 * @see <a
		 * href="http://docs.basho.com/riak/latest/theory/concepts/Vector-Clocks/#Vector-Clock-Pruning">Vector
		 * Clock Pruning</a> for details.
		 */
		public Builder withBigVClock(Long bigVClock)
		{
			this.bigVClock = bigVClock;
			return this;
		}

		/**
		 * Set the chash_keyfun value.
		 *
		 * @param func a Function representing the Erlang func to use.
		 * @return a reference to this object.
		 */
		public Builder withChashkeyFunction(Function func)
		{
			this.chashkeyFunction = func;
			return this;
		}

		/**
		 * Set the last_write_wins value. Unless you really know what you're
		 * doing, you probably do not want to set this to true.
		 *
		 * @param wins whether to ignore vector clocks when writing.
		 * @return a reference to this object.
		 */
		public Builder withLastWriteWins(boolean wins)
		{
			this.lastWriteWins = wins;
			return this;
		}

		/**
		 * Set the linkfun value.
		 *
		 * @param func a Function representing the Erlang func to use.
		 * @return a reference to this object.
		 */
		public Builder withLinkwalkFunction(Function func)
		{
			this.linkWalkFunction = func;
			return this;
		}

		/**
		 * Set the rw value. Individual requests (or buckets in a bucket type)
		 * can override this.
		 *
		 * @param rw the rw value as an integer.
		 * @return a reference to this object.
		 */
		public Builder withRw(int rw)
		{
			this.rw = rw;
			return this;
		}

		/**
		 * Set the dw value. Individual requests (or buckets in a bucket type)
		 * can override this.
		 *
		 * @param dw the dw value as an integer.
		 * @return a reference to this object.
		 */
		public Builder withDw(int dw)
		{
			this.dw = dw;
			return this;
		}

		/**
		 * Set the w value. Individual requests (or buckets in a bucket type)
		 * can override this.
		 *
		 * @param w the w value as an integer.
		 * @return a reference to this object.
		 */
		public Builder withW(int w)
		{
			this.w = w;
			return this;
		}

		/**
		 * Set the r value. Individual requests (or buckets in a bucket type)
		 * can override this.
		 *
		 * @param r the r value as an integer.
		 * @return a reference to this object.
		 */
		public Builder withR(int r)
		{
			this.r = r;
			return this;
		}

		/**
		 * Set the pr value. Individual requests (or buckets in a bucket type)
		 * can override this.
		 *
		 * @param pr the pr value as an integer.
		 * @return a reference to this object.
		 */
		public Builder withPr(int pr)
		{
			this.pr = pr;
			return this;
		}

		/**
		 * Set the pw value. Individual requests (or buckets in a bucket type)
		 * can override this.
		 *
		 * @param pw the pw value as an integer.
		 * @return a reference to this object.
		 */
		public Builder withPw(int pw)
		{
			this.pw = pw;
			return this;
		}

		/**
		 * Set the not_found_ok value. If true a vnode returning notfound for a
		 * key increments the r tally. False is higher consistency, true is
		 * higher availability.
		 *
		 * @param ok the not_found_ok value.
		 * @return a reference to this object.
		 */
		public Builder withNotFoundOk(boolean ok)
		{
			this.notFoundOk = ok;
			return this;
		}

		/**
		 * Add a pre-commit hook. The supplied Function must be an Erlang or
		 * Named JS function.
		 *
		 * @param hook the Function to add.
		 * @return a reference to this object.
		 * @see <a
		 * href="http://docs.basho.com/riak/latest/dev/using/commit-hooks/">Using
		 * Commit Hooks</a>
		 */
		public Builder withPrecommitHook(Function hook)
		{
			if (null == hook || !(!hook.isJavascript() || hook.isNamed()))
			{
				throw new IllegalArgumentException("Must be a named JS or Erlang function.");
			}
			this.preCommitHook = hook;
			return this;
		}

		/**
		 * Add a post-commit hook. The supplied Function must be an Erlang or
		 * Named JS function.
		 *
		 * @param hook the Function to add.
		 * @return a reference to this object.
		 * @see <a
		 * href="http://docs.basho.com/riak/latest/dev/using/commit-hooks/">Using
		 * Commit Hooks</a>
		 */
		public Builder withPostcommitHook(Function hook)
		{
			this.postCommitHook = hook;
			return this;
		}

		/**
		 * Set the old_vclock value.
		 *
		 * @param oldVClock an long representing a epoch time value.
		 * @return a reference to this object.
		 * @see <a
		 * href="http://docs.basho.com/riak/latest/theory/concepts/Vector-Clocks/#Vector-Clock-Pruning">Vector
		 * Clock Pruning</a> for details.
		 */
		public Builder withOldVClock(Long oldVClock)
		{
			this.oldVClock = oldVClock;
			return this;
		}

		/**
		 * Set the young_vclock value.
		 *
		 * @param youngVClock a long representing a epoch time value.
		 * @return a reference to this object.
		 * @see <a
		 * href="http://docs.basho.com/riak/latest/theory/concepts/Vector-Clocks/#Vector-Clock-Pruning">Vector
		 * Clock Pruning</a> for details.
		 */
		public Builder withYoungVClock(Long youngVClock)
		{
			this.youngVClock = youngVClock;
			return this;
		}

		/**
		 * Set the small_vclock value.
		 *
		 * @param smallVClock a long representing a epoch time value.
		 * @return a reference to this object.
		 * @see <a
		 * href="http://docs.basho.com/riak/latest/theory/concepts/Vector-Clocks/#Vector-Clock-Pruning">Vector
		 * Clock Pruning</a> for details.
		 */
		public Builder withSmallVClock(Long smallVClock)
		{
			this.smallVClock = smallVClock;
			return this;
		}

		/**
		 * Set the nVal.
		 *
		 * @param nVal the number of replicas.
		 * @return a reference to this object.
		 */
		public Builder withNVal(int nVal)
		{
			if (nVal <= 0)
			{
				throw new IllegalArgumentException("nVal must be >= 1");
			}
			this.nval = nVal;
			return this;
		}

		/**
		 * Enable Legacy Riak Search. Setting this to true causes the search
		 * pre-commit hook to be added.
		 *
		 * <b>Note this is only for legacy Riak (&lt; v2.0) Search support.</b>
		 *
		 * @param enable add/remove (true/false) the pre-commit hook for Legacy
		 * Riak Search.
		 * @return a reference to this object.
		 */
		public Builder withLegacyRiakSearchEnabled(boolean enable)
		{
			this.legacySearch = enable;
			return this;
		}

		/**
		 * Associate a Search Index. This only applies if Yokozuna is enabled in
		 * Riak v2.0.
		 *
		 * @param indexName The name of the search index to use.
		 * @return a reference to this object.
		 */
		public Builder withSearchIndex(String indexName)
		{
			if (null == indexName || indexName.length() == 0)
			{
				throw new IllegalArgumentException("Index name cannot be null or zero length");
			}
			this.searchIndex = indexName;
			return this;
		}

		public StoreBucketProperties build()
		{
			return new StoreBucketProperties(this);
		}

	}
}

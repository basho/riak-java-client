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
package com.basho.riak.client.operations.mapreduce;

import com.basho.riak.client.query.functions.Function;

/**
 * A reduce phase of a MapReduce job spec. Just a tag class.
 *
 * @author russell
 */
class ReducePhase extends FunctionPhase
{

	/**
	 * Create a Reduce Phase
	 *
	 * @param phaseFunction
	 * @param arg
	 * @param keepResult
	 */
	public ReducePhase(Function phaseFunction, Object arg, Boolean keepResult)
	{
		super(PhaseType.REDUCE, phaseFunction, arg, keepResult);
	}

	/**
	 * @param phaseFunction
	 * @param arg
	 */
	public ReducePhase(Function phaseFunction, Object arg)
	{
		this(phaseFunction, arg, null);
	}

	/**
	 * @param phaseFunction
	 * @param keep
	 */
	public ReducePhase(Function phaseFunction, boolean keep)
	{
		this(phaseFunction, null, keep);
	}

	/**
	 * @param phaseFunction
	 */
	public ReducePhase(Function phaseFunction)
	{
		this(phaseFunction, null, null);
	}

}

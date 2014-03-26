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

/**
 * Shared common interface for Map/Reduce phase definitions.
 *
 * @author russell
 */
abstract class MapReducePhase
{

	/**
	 * Possible phase typess.
	 */
	public enum PhaseType
	{
		LINK("link"), MAP("map"), REDUCE("reduce");

		private final String phaseName;

		private PhaseType(String phaseName)
		{
			this.phaseName = phaseName;
		}

		public String toString()
		{
			return this.phaseName;
		}
	}

	private final PhaseType type;

	protected MapReducePhase(PhaseType type)
	{
		this.type = type;
	}

	/**
	 * Is this phase's output to be returned or only passed as input to the next phase.
	 *
	 * @return true if the results are returned, false otherwise.
	 */
	abstract Boolean isKeep();

	/**
	 * The PhaseType of this {@link MapReducePhase} implementation.
	 *
	 * @return a PhaseType.
	 */
	PhaseType getType()
	{
		return type;
	}
}

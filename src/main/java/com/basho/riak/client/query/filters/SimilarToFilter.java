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
package com.basho.riak.client.query.filters;


/**
 * Filter in keys that are within a specified Levenshtein edit distance of the provided string
 *
 * @author russell
 */
public class SimilarToFilter extends KeyFilter
{
	private static final String NAME = "similar_to";
	private final String value;
	private final int distance;

	/**
	 * @param value           the string to compare to
	 * @param maxEditDistance
	 */
	public SimilarToFilter(String value, int maxEditDistance)
	{
		super(NAME);
		this.value = value;
		this.distance = maxEditDistance;
	}

	public String getValue()
	{
		return value;
	}

	public int getDistance()
	{
		return distance;
	}
}

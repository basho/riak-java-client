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
 * Filter in keys that =< than the configured value.
 *
 * @author russell
 */
public class LessThanOrEqualFilter<T> extends KeyFilter
{
	private static final String NAME = "less_than_eq";
	private final T value;

	/**
	 * @param value
	 */
	public LessThanOrEqualFilter(T value)
	{
		super(NAME);
		this.value = value;
	}

	public T getValue()
	{
		return value;
	}
}

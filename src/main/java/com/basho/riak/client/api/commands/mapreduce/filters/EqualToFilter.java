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
package com.basho.riak.client.api.commands.mapreduce.filters;


/**
 * Filter in keys that equal the configured value
 * @author russell
 *
 */
public class EqualToFilter<T> extends KeyFilter
{

    private final static String NAME = "eq";
    private final T value;

    /**
     * Filter in keys whose name is <code>equalTo</code>
     * @param value
     */
    public EqualToFilter(T value)
	{
	    super(NAME);
	    this.value = value;
    }

	public T getValue()
	{
		return value;
	}
}

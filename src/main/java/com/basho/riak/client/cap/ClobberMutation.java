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
package com.basho.riak.client.cap;

/**
 * A mutation that overwrites the original value with a new one.
 *
 * @author russell
 * @param <T> the type of the value
 *
 */
public class ClobberMutation<T> implements Mutation<T> {

  final T newValue;

  /**
   * Create a clobber mutation that will return <code>newValue</code> from <code>apply(T)</code>
   * @param newValue
   */
  public ClobberMutation(T newValue) {
    this.newValue = newValue;
  }

  /**
   * @return simply returns whatever the argument to the constructor was.
   */
  public T apply(T original) {
    return newValue;
  }
}
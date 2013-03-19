/*
 * Copyright 2013 Basho Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basho.riak.client.cap;

/**
 * Extends {@link Mutation} and allows for a store operation not to occur 
 * if the object it is being applied to is not modified.
 * 
 * <p>
 * By implementing this interface you can avoid a store operation from occurring
 * if the object it is be applied to is not modified. 
 * </p>
 * <p>For example:
 * <code><pre>
 * Mutation&lt;IRiakObject&gt m = new ConditionalStoreMutation&lt;IRiakObject&gt;() {
 *     private boolean modified;
 * 
 *     {@literal @}Override
 *     IRiakObject apply(IRiakObject original) {
 *          // I didn't do anything!
 *          modifed = false; 
 *     }
 * 
 *     {@literal @}Overrive
 *     boolean hasMutated() {
 *         return modified;
 *     }
 * };
 * </code></pre></p>
 * 
 * 
 * @author gmedina
 */
public interface ConditionalStoreMutation<T> extends Mutation<T>
{
    public boolean hasMutated();
}

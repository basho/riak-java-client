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
/**
 * Classes for converting Riak data into domain specific classes and back.
 * 
 * <p>
 * If you need domain logic to reason about conflicts in your data it is better
 * if your data is in your domain classes, implement
 * {@link com.basho.riak.client.convert.Converter} and pass it to any
 * {@link com.basho.riak.client.operations.RiakOperation} on data to convert to
 * and from {@link com.basho.riak.client.IRiakObject} your domain classes.
 * </p>
 * <p>
 * {@link com.basho.riak.client.convert.JSONConverter} is a default
 * implementation that uses Jackson JSON library.
 * See <a href="http://wiki.fasterxml.com/JacksonHome">Jackson</a>
 * </p>
 * 
 * @see com.basho.riak.client.convert.JSONConverter
 * 
 */
package com.basho.riak.client.convert;
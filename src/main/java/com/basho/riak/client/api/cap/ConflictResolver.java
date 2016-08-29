/*
 * Copyright 2013 Basho Technologies Inc.
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
package com.basho.riak.client.api.cap;

import java.util.List;

/**
 * Interface used to resolve siblings.
 * <p>
 * When you have multiple writers there may be multiple versions of the same
 * object stored in Riak. When fetching all of these will be returned and you
 * will need to resolve the conflict.
 * </p>
 * <p>
 * To facilitate this, you can store an instance of the ConflictResolver
 * in the {@link com.basho.riak.client.api.cap.ConflictResolverFactory} for a class.
 * It will then be used by the {@link com.basho.riak.client.api.commands.kv.FetchValue.Response}
 * to resolve a set a of siblings to a single object.
 * </p>
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 * @param <T> The type of the objects to be resolved.
 */
public interface ConflictResolver<T>
{
    T resolve(List<T> objectList) throws UnresolvedConflictException;
}

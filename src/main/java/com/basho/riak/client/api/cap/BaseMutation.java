/*
 * Copyright 2013 Basho Technologies Inc
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

public abstract class BaseMutation<T> implements Mutation<T>
{

    private boolean hasMutated = false;

    protected void setHasMutated(boolean hasMutated)
    {
        this.hasMutated = hasMutated;
    }

    public boolean hasMutated()
    {
        return hasMutated;
    }

    @Override
    public abstract T apply(T original);

}

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
import java.util.concurrent.ExecutionException;

/**
 * 
 * Holds the Collection of siblings so they can be presented for further resolution attempts.
 * 
 * @author Russell Brown <russelldb at basho dot com>
 * @since 1.0
 */
public class UnresolvedConflictException extends ExecutionException
{

    /**
     * eclipse generated id
     */
    private static final long serialVersionUID = -219858468775752064L;

    private final List<? extends Object> siblings;

    /**
     * For when a list of siblings cannot be whittled down to one.
     * 
     * @param cause
     *            the exception that broke the camels back
     * @param message
     *            a String message
     * @param siblings
     *            the list of siblings
     */
    public UnresolvedConflictException(Throwable cause, String message, List<? extends Object> siblings)
    {
        super(message, cause);
        this.siblings = siblings;
    }

    /**
     * For when a collection of siblings cannot be whittled down to one.
     * 
     * @param message
     *            a String message
     * @param siblings
     *            the list of siblings
     */
    public UnresolvedConflictException(String message, List<? extends Object> siblings)
    {
        super(message);
        this.siblings = siblings;
    }

    /**
     * Get the siblings that could not be resolved.
     * @return the siblings
     */
    public List<? extends Object> getSiblings()
    {
        return siblings;
    }
}
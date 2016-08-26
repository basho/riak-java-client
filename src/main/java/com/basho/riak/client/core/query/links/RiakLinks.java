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
package com.basho.riak.client.core.query.links;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A thread safe container for {@link RiakLink} objects.
 * 
 * <br/><b>Thread Safety:</b><br/> This is a thread safe container.
 *
 * @author Brian Roach <roach at basho dot com>
 * @see RiakLink
 * @since 2.0
 */
public class RiakLinks implements Iterable<RiakLink>
{
    private final Set<RiakLink> links = Collections.newSetFromMap(new ConcurrentHashMap<RiakLink, Boolean>());

    /**
     * Reports if there are any {@code RiakLink} objects present
     *
     * @return {@code true} if there are links present, {@code false} otherwise
     */
    public boolean isEmpty()
    {
        return links.isEmpty();
    }

    /**
     * Returns the number of links present
     *
     * @return the number of links present
     */
    public int size()
    {
        return links.size();
    }

    /**
     * Determine if a specific {@code RiakLink} is present.
     *
     * @param link The {@code RiakLink} to check for.
     * @return {@code true} if the link is present, {@code false} otherwise
     */
    public boolean hasLink(RiakLink link)
    {
        return links.contains(link);
    }

    /**
     * Add {@link RiakLink}s
     *
     * @param links a Collection of RiakLink objects to add
     * @return a reference to this object
     */
    public RiakLinks addLinks(Collection<RiakLink> links)
    {
        this.links.addAll(links);
        return this;
    }

    /**
     * Adds a {@link RiakLink}
     *
     * @param link a {@code RiakLink} to be added
     * @return a reference to this object
     */
    public RiakLinks addLink(RiakLink link)
    {
        links.add(link);
        return this;
    }

    /**
     * Remove a {@code RiakLink}
     *
     * @param link the {@code RiakLink} to remove
     * @return {@code true} if the link was present and was removed, {@code false} otherwise
     */
    public boolean removeLink(RiakLink link)
    {
        return links.remove(link);
    }

    /**
     * Remove all links
     */
    public void removeAllLinks()
    {
        links.clear();
    }

    /**
     * Return all the links.
     * <p>
     * Changes to the returned set do not modify this container.
     * </p>
     *
     * @return the set of RiakLink objects
     */
    public Set<RiakLink> getLinks()
    {
        return new HashSet<>(links);
    }

    /**
     * Return an iterator
     *
     * @return an {@code Iterator} that returns the links in this container
     */
    @Override
    public Iterator<RiakLink> iterator()
    {
        return links.iterator();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        RiakLinks riakLinks = (RiakLinks) o;
        return links.equals(riakLinks.links);
    }

    @Override
    public int hashCode()
    {
        return links.hashCode();
    }

    @Override
    public String toString()
    {
        return "RiakLinks{" + "links: " + links + '}';
    }
}

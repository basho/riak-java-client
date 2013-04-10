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
package com.basho.riak.client.core;

import com.basho.riak.client.core.RiakNode.State;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * The default {@link NodeManager} used by {@link RiakCluster} if none is 
 * specified.
 * 
 * This is a basic round-robin load-balancer that will remove nodes from the 
 * healthy list if they report they are health checking and replace them when
 * they report they are running again.
 * 
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class DefaultNodeManager implements NodeManager, NodeStateListener
{
    private final ArrayList<RiakNode> healthy = new ArrayList<>();
    private final ArrayList<RiakNode> unhealthy = new ArrayList<>();
    private volatile int current = 0;
    private final Logger logger = LoggerFactory.getLogger(DefaultNodeManager.class);
    
    @Override
    public void init(List<RiakNode> nodes)
    {
        healthy.addAll(nodes);
    }

    @Override
    public RiakNode selectNode(RiakNode previous) 
    {
        synchronized(healthy)
        {
            if (healthy.size() > 1)
            {
                RiakNode next = previous;
                while (next == previous)
                {
                    next = healthy.get(Math.abs(current % healthy.size()));
                    current++;
                }
                return next;
            }
            else if (healthy.size() == 1)
            {
                return healthy.get(0);
            }
            else 
            {
                return null;
            }
        }
    }

    @Override
    public void nodeStateChanged(RiakNode node, State state)
    {
        switch (state)
        {
            case RUNNING:
                synchronized(healthy)
                {
                    if (unhealthy.remove(node))
                    {
                        healthy.add(node);
                        logger.info("NodeManager moved node to healthy list; {}", 
                                    node.getRemoteAddress());
                    }
                }
                break;
            case HEALTH_CHECKING:
                synchronized(healthy)
                {
                    if (healthy.remove(node))
                    {
                        unhealthy.add(node);
                        logger.info("NodeManager moved node to unhealthy list; {}", 
                                    node.getRemoteAddress());
                    }
                }
                break;
            case SHUTTING_DOWN:
            case SHUTDOWN:
                synchronized(healthy)
                {
                    healthy.remove(node);
                    unhealthy.remove(node);
                }
                logger.info("NodeManager removed node due to it shutting down; {}",
                                node.getRemoteAddress());
                break;
            default:
                break;
        }
    }

    @Override
    public void addNode(RiakNode newNode)
    {
        synchronized(healthy)
        {
            healthy.add(newNode);
        }
        
    }

    @Override
    public boolean removeNode(RiakNode node)
    {
        boolean removed = false;
        synchronized(healthy)
        {
            removed = healthy.remove(node);
            if (!removed)
            {
                removed = unhealthy.remove(node);
            }
        }
        if (removed)
        {
            node.removeStateListener(this);
            node.shutdown();
            logger.info("NodeManager removed and shutdown node; {}", 
                        node.getRemoteAddress());
        }
        return removed;
    }
    
}

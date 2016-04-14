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
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * The default {@link NodeManager} used by {@link RiakCluster} if none is
 * specified.
 *
 * This NodeManager round-robins through a list of {@link RiakNode}s and attempts
 * to execute the operation passed to it. If a node reports that it is
 * health checking it is removed from the list until it sends an update that it
 * is again running. If the selected node cannot accept the operation because all
 * connections are in use or it unable to make a new connection, the next node in
 * the list is tried until either the operation is accepted or all nodes have
 * been tried. If no nodes are able to accept the operation its setException()
 * method is called with a {@link NoNodesAvailableException}.
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class DefaultNodeManager implements NodeManager, NodeStateListener
{
    private final ArrayList<RiakNode> healthy = new ArrayList<RiakNode>();
    private final ArrayList<RiakNode> unhealthy = new ArrayList<RiakNode>();
    private final AtomicInteger index = new AtomicInteger();
    private final Logger logger = LoggerFactory.getLogger(DefaultNodeManager.class);
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    @Override
    public void init(List<RiakNode> nodes)
    {
        try
        {
            lock.writeLock().lock();
            healthy.addAll(nodes);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    @Override
    public boolean executeOnNode(FutureOperation operation, RiakNode previousNode)
    {
        try
        {
            lock.readLock().lock();
            boolean executed = false;
            if (healthy.size() > 1)
            {
                int startIndex = index.getAndIncrement();
                int currentIndex = startIndex;

                do
                {
                    if (healthy.get(Math.abs(currentIndex % healthy.size())).execute(operation))
                    {
                        executed = true;
                        break;
                    }
                    currentIndex++;
                }
                while (Math.abs(currentIndex % healthy.size()) != Math.abs(startIndex % healthy.size()));
            }
            else if (healthy.size() == 1)
            {
                executed = healthy.get(0).execute(operation);
            }

            return executed;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    @Override
    public void nodeStateChanged(RiakNode node, State state)
    {
        switch (state)
        {
            case RUNNING:
                try
                {
                    lock.writeLock().lock();
                    if (unhealthy.remove(node))
                    {
                        healthy.add(node);
                        logger.info("NodeManager moved node to healthy list; {}:{}",
                                    node.getRemoteAddress(), node.getPort());
                    }
                }
                finally
                {
                    lock.writeLock().unlock();
                }
                break;
            case HEALTH_CHECKING:
                try
                {
                    lock.writeLock().lock();
                    if (healthy.remove(node))
                    {
                        unhealthy.add(node);
                        logger.info("NodeManager moved node to unhealthy list; {}:{}",
                                    node.getRemoteAddress(), node.getPort());
                    }
                }
                finally
                {
                    lock.writeLock().unlock();
                }
                break;
            case SHUTTING_DOWN:
            case SHUTDOWN:
                boolean removed = false;
                try
                {
                    lock.writeLock().lock();
                    removed = healthy.remove(node);
                    if (!removed)
                    {
                        unhealthy.remove(node);
                    }
                }
                finally
                {
                    lock.writeLock().unlock();
                }
                if (removed)
                {
                    logger.info("NodeManager removed node due to it shutting down; {}:{}",
                                node.getRemoteAddress(), node.getPort());
                }
                break;
            default:
                break;
        }
    }

    @Override
    public void addNode(RiakNode newNode)
    {
        try
        {
            lock.writeLock().lock();
            healthy.add(newNode);
        }
        finally
        {
            lock.writeLock().unlock();
        }

    }

    @Override
    public boolean removeNode(RiakNode node)
    {
        boolean removed;
        try
        {
            lock.writeLock().lock();
            removed = healthy.remove(node);
            if (!removed)
            {
                removed = unhealthy.remove(node);
            }
        }
        finally
        {
            lock.writeLock().unlock();
        }

        if (removed)
        {
            node.removeStateListener(this);
            node.shutdown();
            logger.info("NodeManager removed and shutdown node; {}:{}",
                        node.getRemoteAddress(), node.getPort());
        }
        return removed;
    }
}

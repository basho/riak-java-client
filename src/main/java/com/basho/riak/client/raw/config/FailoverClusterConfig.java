/*
 * Copyright (c) 2002 - 2012. Temetra Ltd. All Rights Reserved
 */

package com.basho.riak.client.raw.config;

public abstract class FailoverClusterConfig<T extends Configuration> extends ClusterConfig<T> implements Configuration
{
  public static final long DEFAULT_FAILEDNODEMILLISBETWEENRETRIES=15000L;
  public static final long DEFAULT_RETRYIOEXCEPTIONTIMEOUT=2000L;
  public static final long DEFAULT_PAUSEBETWEENALLDEADNODES=50L;

  private long failedNodeMillisBetweenRetries=DEFAULT_FAILEDNODEMILLISBETWEENRETRIES;
  private long retryIOExceptionTimeout=DEFAULT_RETRYIOEXCEPTIONTIMEOUT;
  private long pauseBetweenAllDeadNodes=DEFAULT_PAUSEBETWEENALLDEADNODES;

  public FailoverClusterConfig(int totalMaximumConnections) {
    super(totalMaximumConnections);
  }

  protected abstract ClusterConfig<T> addHosts(String... hosts);
  protected abstract ClusterConfig<T> addHosts(T config, String... hosts);

  public FailoverClusterConfig withFailedNodeRetryInterval(long millisBetweenRetries)
  {
    this.failedNodeMillisBetweenRetries=millisBetweenRetries;
    return this;
  }

  public FailoverClusterConfig withretryIOExceptionTimeout(long retryIOExceptionTimeout)
  {
    this.retryIOExceptionTimeout=retryIOExceptionTimeout;
    return this;
  }

  public FailoverClusterConfig withPauseBetweenAllDeadNodes(long pauseBetweenAllDeadNodes)
  {
    this.pauseBetweenAllDeadNodes=pauseBetweenAllDeadNodes;
    return this;
  }

  public long getFailedNodeMillisBetweenRetries()
  {
    return failedNodeMillisBetweenRetries;
  }

  public long getRetryIOExceptionTimeout()
  {
    return retryIOExceptionTimeout;
  }

  public long getPauseBetweenAllDeadNodes()
  {
    return pauseBetweenAllDeadNodes;
  }
}

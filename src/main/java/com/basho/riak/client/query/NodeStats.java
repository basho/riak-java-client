/*
 * Copyright 2012 roach.
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
package com.basho.riak.client.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * The encapsulation of the data returned by the Riak <code>/stats</code> 
 * operation.
 * <p>
 * By implementing the {@link Iterable<NodeStats>} interface it contains N sets
 * of data where N is the number of connections the current client holds.
 * <p>
 * For example, using the HTTPClusterClient you can retrieve stats from all of your nodes:
 * </p>
 * <code><pre>
 * HTTPClusterConfig c = new HTTPClusterConfig(10);
 * HTTPClientConfig cc HTTPClientConfig.defaults();
 * c.addHosts(cc,"192.168.1.5:8098","192.168.1.6:8098","192.168.1.7:8098");
 * 
 * IRiakClient riakClient = RiakFactory.newClient(c);
 * 
 * for (NodeStats ns : riakClient.stats())
 * {
 *     System.out.println(ns.nodename());
 *     Syste.out.println(ns.vnodeGets());
 * }
 * 
 * </pre></code
 * 
 * @author roach
 */
public class NodeStats implements Iterable<NodeStats>
{
    private final JSONObject stats;
    
    /*
     * This got rather tricky due to the HTTPClusterClient implementing RawClient
     * and then using a pool of HTTClientAdapter objects which also implemented RawClient. 
     * I either had to make both return a List which meant the HTTPClientAdapter
     * would always return a List containing one thing, then I'd have to combine
     * those lists into one List in the HTTPClusterClient, or ... I could make 
     * this object Iterable. I went with the latter, implementing it as a 
     * psudo double-linked list 
     */
    
    private NodeStats next;
    private NodeStats previous;
    
    
    public NodeStats (JSONObject stats) 
    {
        this.stats = stats;
    }    
    
    /**
     * Returns the <code>vnode_gets</code> value from the Riak stats reply
     * @return int value
     */
    public int vnodeGets() 
    {
        return getIntValue("vnode_gets");
    }
 
    /**
     * Returns the <code>vnode_gets</code> value from the Riak stats reply
     * @return <code>int</code> value
     */
    public int vnodePuts()
    {
        return getIntValue("vnode_puts");
    }
    
    /**
     * Returns the <code>vnode_index_reads</code> value from the Riak stats reply
     * @return <code>int</code> value
     */
    public int vnodeIndexReads()
    {
        return getIntValue("vnode_index_reads"); 
    }
    
    /**
     * Returns the <code>vnode_index_writes</code> value from the Riak stats reply
     * @return <code>int</code> value
     */
    public int vnodeIndexWrites()
    {
        return getIntValue("vnode_index_writes");
    }
    
    /**
     * Returns the <code>vnode_index_writes_postings</code> value from the Riak stats reply
     * @return <code>int</code> value
     */
    public int vnodeIndexWritePostings()
    {
        return getIntValue("vnode_index_writes_postings");
    }
    
    /**
     * Returns the <code>vnode_index_deletes</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int vnodeIndexDeletes()
    {
        return getIntValue("vnode_index_deletes");
    }
    
    /**
     * Returns the <code>vnode_index_deletes_postings</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int vnodesIndexDeletesPostings()
    {
        return getIntValue("vnode_index_deletes_postings");
    }
    
    /**
     * Returns the <code>read_repairs</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int readRepairs()
    {
        return getIntValue("read_repairs");
    }
    
    /**
     * Returns the <code>vnode_gets_total</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int vnodeGetsTotal()
    {
        return getIntValue("vnode_gets_total");
    }
    
    /**
     * Returns the <code>vnode_puts_total</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int vnodePutsTotal()
    {
        return getIntValue("vnode_puts_total");
    }
    
    /**
     * Returns the <code>vnode_index_reads_total</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int vnodeIndexReadsTotal()
    {
        return getIntValue("vnode_index_reads_total");
    }
    
    /**
     * Returns the <code>vnode_index_writes_total</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int vnodeIndexWritesTotal()
    {
        return getIntValue("vnode_index_writes_total");
    }
    
    /**
     * Returns the <code>vnode_index_writes_postings_total</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int vnodeIndexWritesPostingsTotal()
    {
        return getIntValue("vnode_index_writes_postings_total");
    }
    
    /**
     * Returns the <code>vnode_index_deletes_total</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int vnodeIndexDeletesTotal()
    {
        return getIntValue("vnode_index_deletes_total");
    }
    
    /**
     * Returns the <code>vnode_index_deletes_postings_total</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int vnodeIndexDeletesPostingsTotal()
    {
        return getIntValue("vnode_index_deletes_postings_total");
    }
    
    /**
     * Returns the <code>node_gets</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int nodeGets()
    {
        return getIntValue("node_gets");
    }
    
    /**
     * Returns the <code>node_gets_total</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int nodeGetsTotal()
    {
        return getIntValue("node_gets_total");
    }
    
    /**
     * Returns the <code>node_get_fsm_time_mean</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int nodeGetFsmTimeMean()
    {
        return getIntValue("node_get_fsm_time_mean");
    }
    
    /**
     * Returns the <code>node_get_fsm_time_median</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int nodeGetFsmTimeMedian()
    {
        return getIntValue("node_get_fsm_time_median");
    }
    
    /**
     * Returns the <code>node_get_fsm_time_95</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int nodeGetFsmTime95()
    {
        return getIntValue("node_get_fsm_time_95");
    }
    
    /**
     * Returns the <code>node_get_fsm_time_99</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int nodeGetFsmTime99()
    {
        return getIntValue("node_get_fsm_time_99");
    }
    
    /**
     * Returns the <code>node_get_fsm_time_100</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int nodeGetFsmTime100()
    {
        return getIntValue("node_get_fsm_time_100");
    }
    
    /**
     * Returns the <code>node_puts</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int nodePuts()
    {
        return getIntValue("node_puts");
    }
    
    /**
     * Returns the <code>node_puts_total</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int nodePutsTotal()
    {
        return getIntValue("node_puts_total");
    }
    
    /**
     * Returns the <code>node_get_fsm_time_mean</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int nodePutFsmTimeMean()
    {
        return getIntValue("node_get_fsm_time_mean");
    }
    
    /**
     * Returns the <code>node_put_fsm_time_median</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int nodePutFsmTimeMedian()
    {
        return getIntValue("node_put_fsm_time_median");
    }
    
    /**
     * Returns the <code>node_put_fsm_time_95</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int nodePutFsmTime95()
    {
        return getIntValue("node_put_fsm_time_95");
    }
    
    /**
     * Returns the <code>node_put_fsm_time_99</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int nodePutFsmTime99()
    {
        return getIntValue("node_put_fsm_time_99");
    }
    
    /**
     * Returns the <code>node_put_fsm_time_100</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int nodePutFsmTime100()
    {
        return getIntValue("node_put_fsm_time_100");
    }
    
    /**
     * Returns the <code>node_get_fsm_siblings_mean</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int nodePutFsmSiblingsMean()
    {
        return getIntValue("node_get_fsm_siblings_mean");
    }
    
    /**
     * Returns the <code>node_put_fsm_siblings_median</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int nodePutFsmSiblingsMedian()
    {
        return getIntValue("node_put_fsm_siblings_median");
    }
    
    /**
     * Returns the <code>node_put_fsm_siblings_95</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int nodePutFsmSiblings95()
    {
        return getIntValue("node_put_fsm_siblings_95");
    }
    
    /**
     * Returns the <code>node_put_fsm_siblings_99</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int nodePutFsmSiblings99()
    {
        return getIntValue("node_put_fsm_siblings_99");
    }
    
    /**
     * Returns the <code>node_put_fsm_siblings_100</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int nodePutFsmSiblings100()
    {
        return getIntValue("node_put_fsm_siblings_100");
    }
    
    /**
     * Returns the <code>node_get_fsm_objsize_mean</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int nodePutFsmObjsizeMean()
    {
        return getIntValue("node_get_fsm_objsize_mean");
    }
    
    /**
     * Returns the <code>node_put_fsm_objsize_median</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int nodePutFsmObjsizeMedian()
    {
        return getIntValue("node_put_fsm_objsize_median");
    }
    
    /**
     * Returns the <code>node_put_fsm_objsize_95</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int nodePutFsmObjsize95()
    {
        return getIntValue("node_put_fsm_objsize_95");
    }
    
    /**
     * Returns the <code>node_put_fsm_objsize_99</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int nodePutFsmObjsize99()
    {
        return getIntValue("node_put_fsm_objsize_99");
    }
    
    /**
     * Returns the <code>node_put_fsm_objsize_100</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int nodePutFsmSObjsize100()
    {
        return getIntValue("node_put_fsm_objsize_100");
    }
    
    /**
     * Returns the <code>read_repairs_total</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int readRepairsTotal()
    {
        return getIntValue("read_repairs_total");
    }
    
    /**
     * Returns the <code>coord_redirs_total</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int coordRedirsTotal()
    {
        return getIntValue("coord_redirs_total");
    }
    
    /**
     * Returns the <code>cpu_nprocs</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int cpuNumProcs()
    {
        return getIntValue("cpu_nprocs");
    }
    
    /**
     * Returns the <code>cpu_avg1</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int cpuAvg1()
    {
        return getIntValue("cpu_avg1");
    }
    
    /**
     * Returns the <code>cpu_avg5</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int cpuAvg5()
    {
        return getIntValue("cpu_avg5");
    }

    /**
     * Returns the <code>cpu_avg15</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int cpuAvg15()
    {
        return getIntValue("cpu_avg15");
    }
    
    /**
     * Returns the <code>mem_total</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int memTotal()
    {
        return getIntValue("mem_total");
    }
    
    /**
     * Returns the <code>mem_allocated</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int memAllocated()
    {
        return getIntValue("mem_allocated");
    }
    
    /**
     * Returns the <code>nodename</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String nodename()
    {
        return getStringValue("nodename");
    }
    
    /**
     * Returns the <code>connected_nodes</code> value from the Riak Stats reply
     * @return <code>List<String></code> of node names
     */
    public List<String> connectedNodes()
    {
        return getStringList("connected_nodes");
    }
    
    /**
     * Returns the <code>sys_driver_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String sysDriverVersion()
    {
        return getStringValue("sys_driver_version");
    }
    
    /**
     * Returns the <code>sys_global_heaps_size</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int sysGlobalHeapsSize()
    {
        return getIntValue("sys_global_heaps_size");
    }
    
    /**
     * Returns the <code>sys_heap_type</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String sysHeapType()
    {
        return getStringValue("sys_heap_type");
    }
    
    /**
     * Returns the <code>sys_logical_processors</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int sysLogicalProcessors()
    {
        return getIntValue("sys_logical_processors");
    }
    
    /**
     * Returns the <code>sys_otp_release</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String sysOtpRelease()
    {
        return getStringValue("sys_otp_release");
    }
    
    /**
     * Returns the <code>sys_process_count</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int sysProcessCount()
    {
        return getIntValue("sys_process_count");
    }
    
    /**
     * Returns the <code>sys_smp_support</code> value from the Riak Stats reply
     * @return <code>boolean</code> value
     */
    public boolean sysSmpSupport()
    {
        return getBoolValue("sys_smp_support");
    }
    
    /**
     * Returns the <code>sys_system_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String sysSystemVersion()
    {
        return getStringValue("sys_system_version");
    }
    
    /**
     * Returns the <code>sys_system_architecture</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String sysSystemArchitecture()
    {
        return getStringValue("sys_system_architecture");
    }
    
    /**
     * Returns the <code>sys_threads_enabled</code> value from the Riak Stats reply
     * @return <code>boolean</code> value
     */
    public boolean sysThreadsEnabled()
    {
        return getBoolValue("sys_threads_enabled");
    }
    
    /**
     * Returns the <code>sys_thread_pool_size</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int sysThreadPoolSize()
    {
        return getIntValue("sys_thread_pool_size");
    }
    
    /**
     * Returns the <code>sys_wordsize</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int sysWordSize()
    {
        return getIntValue("sys_wordsize");
    }
    
    /**
     * Returns the <code>ring_members</code> value from the Riak Stats reply
     * @return <code>List<String></code> of node names
     */
    public List<String> ringMembers()
    {
        return getStringList("ring_members");
    }
    
    /**
     * Returns the <code>ring_num_partitions</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int ringNumPartitions()
    {
        return getIntValue("ring_num_partitions");
    }
    
    /**
     * Returns the <code>ring_ownership</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String ringOwnership()
    {
        return getStringValue("ring_ownership");
    }
    
    /**
     * Returns the <code>ring_creation_size</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int ringCreationSize()
    {
        return getIntValue("ring_creation_size");
    }
    
    /**
     * Returns the <code>storage_backend</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String storageBackend()
    {
        return getStringValue("storage_backend");
    }
    
    /**
     * Returns the <code>pbc_connects_total</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int pbcConnectsTotal()
    {
        return getIntValue("pbc_connects_total");
    }
    
    /**
     * Returns the <code>pbc_connects</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int pbcConnects()
    {
        return getIntValue("pbc_connects");
    }
    
    /**
     * Returns the <code>pbc_active</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int pbcActive()
    {
        return getIntValue("pbc_active");
    }
    
    /**
     * Returns the <code>ssl_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String sslVeriosn()
    {
        return getStringValue("ssl_version");
    }
    
    /**
     * Returns the <code>public_key_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String publicKeyVersion()
    {
        return getStringValue("public_key_version");
    }
    
    /**
     * Returns the <code>runtime_tools_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String runtimeToolsVersion()
    {
        return getStringValue("runtime_tools_version");
    }
    
    /**
     * Returns the <code>basho_stats_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String bashoStatsVersion()
    {
        return getStringValue("basho_stats_version");
    }
    
    /**
     * Returns the <code>riak_search_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String riakSearchVersion()
    {
        return getStringValue("riak_search_version");
    }
    
    /**
     * Returns the <code>merge_index_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String mergeIndexVersion()
    {
        return getStringValue("merge_index_version");
    }
    
    /**
     * Returns the <code>luwak_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String luwakVersion()
    {
        return getStringValue("luwak_version");
    }
    
    /**
     * Returns the <code>skerl_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String skerlVersion()
    {
        return getStringValue("skerl_version");
    }
    
    /**
     * Returns the <code>riak_kv_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String riakKvVersion()
    {
        return getStringValue("riak_kv_version");
    }
    
    /**
     * Returns the <code>bitcask_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String bitcaskVersion()
    {
        return getStringValue("bitcask_version");
    }
    
    /**
     * Returns the <code>luke_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String lukeVeriosn()
    {
        return getStringValue("luke_version");
    }
    
    /**
     * Returns the <code>erlang_js_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String erlangJsVersion()
    {
        return getStringValue("erlang_js_version");
    }
    
    /**
     * Returns the <code>mochiweb_value</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String mochiwebVersion()
    {
        return getStringValue("mochiweb_value");
    }
    
    /**
     * Returns the <code>inets_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String inetsVersion()
    {
        return getStringValue("inets_version");
    }
    
    /**
     * Returns the <code>riak_pipe_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String riakPipeVersion()
    {
        return getStringValue("riak_pipe_version");
    }
    
    /**
     * Returns the <code>riak_core_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String riakCoreVersion()
    {
        return getStringValue("riak_core_version");
    }
    
    /**
     * Returns the <code>riak_sysmon_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String riak_sysmon_version()
    {
        return getStringValue("riak_sysmon_version");
    }
    
    /**
     * Returns the <code>webmachine_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String webmachineVersion()
    {
        return getStringValue("webmachine_version");
    }
    
    /**
     * Returns the <code>crypto_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String cryptoVersion()
    {
        return getStringValue("crypto_version");
    }
    
    /**
     * Returns the <code>os_mon_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String osMonVersion()
    {
        return getStringValue("os_mon_version");
    }
    
    /**
     * Returns the <code>cluster_info_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String clusterInfoVersion()
    {
        return getStringValue("cluster_info_version");
    }
    
    /**
     * Returns the <code>sasl_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String saslVersion()
    {
        return getStringValue("sasl_version");
    }
    
    /**
     * Returns the <code>lager_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String lagerVersion()
    {
        return getStringValue("lager_version");
    }
    
    /**
     * Returns the <code>basho_lager_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String bashoMetricsVersion()
    {
        return getStringValue("basho_lager_version");
    }
    
    /**
     * Returns the <code>stdlib_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String stdlibVersion()
    {
        return getStringValue("stdlib_version");
    }
    
    /**
     * Returns the <code>kernel_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String kernelVersion()
    {
        return getStringValue("kernel_version");
    }
    
    /**
     * Returns the <code>executing_mappers</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int executingMappers()
    {
        return getIntValue("executing_mappers");
    }
    
    /**
     * Returns the <code>memory_total</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int memoryTotal()
    {
        return getIntValue("memory_total");
    }
    
    /**
     * Returns the <code>memory_processes</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int memoryProcesses()
    {
        return getIntValue("memory_processes");
    }
    
    /**
     * Returns the <code>memory_processes_used</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int memoryProcessesUsed()
    {
        return getIntValue("memory_processes_used");
    }
    
    /**
     * Returns the <code>memory_system</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int memorySystem()
    {
        return getIntValue("memory_system");
    }
    
    /**
     * Returns the <code>memory_atom</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int memoryAtom()
    {
        return getIntValue("memory_atom");
    }
    
    /**
     * Returns the <code>memory_atom_used</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int memoryAtomUsed()
    {
        return getIntValue("memory_atom_used");
    }
    
    /**
     * Returns the <code>memory_binary</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int memoryBinary()
    {
        return getIntValue("memory_binary");
    }
    
    /**
     * Returns the <code>memory_code</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int memoryCode()
    {
        return getIntValue("memory_code");
    }
    
    /**
     * Returns the <code>memory_ets</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public int memoryEts()
    {
        return getIntValue("memory_ets");
    }
    
    private boolean getBoolValue(String key)
    {
        try
        {
            return stats.getBoolean(key);
        }
        catch (JSONException ex)
        {
            return false;
        }
    }
    
    private List<String> getStringList(String key)
    {
        try
        {
            JSONArray array = stats.getJSONArray(key);
            ArrayList<String> list = new ArrayList<String>();
            for (int i = 0; i < array.length(); i++)
            {
                list.add(array.getString(i));
            }
            
            return list;
            
        }
        catch (JSONException ex)
        {
            return new ArrayList<String>();
        }
        
    }
    
    private String getStringValue(String key)
    {
        try
        {
            return stats.getString(key);
        }
        catch (JSONException ex)
        {
            return "";
        }
    }
    
    private int getIntValue(String key) 
    {
        try
        {
            return stats.getInt(key);
        }
        catch (JSONException ex)
        {
            return 0;
        }
    }
    
    
    NodeStats getPrevios()
    {
        return previous;
    }
    
    NodeStats getNext()
    {
        return next;
    }
    
    void setNext(NodeStats anotherStats)
    {
        this.next = anotherStats;
    }
    
    void setPrevious(NodeStats anotherStats)
    {
        this.previous = anotherStats;
    }
    
    /**
     * Adds a set of stats to the end of the collection. Using the iterator
     * you can retrieve each set in order
     * @param anotherStats an additional NodeStats
     */
    public void add(NodeStats anotherStats)
    {
        if (next == null)
        {
            next = anotherStats;
            anotherStats.setPrevious(this);
        }
        else
        {
            NodeStats tmp = next;
            while (tmp.getNext() != null)
            {
                tmp = tmp.getNext();
            }
            
            tmp.setNext(anotherStats);
            anotherStats.setPrevious(tmp);
        }
    }

    
    
    public Iterator<NodeStats> iterator()
    {
        NodeStats first;
        
        if (previous == null)
        {
            first = this;
        }
        else
        {
            first = previous;
            while (first.getPrevios() != null)
            {
                first = first.getPrevios();
            }
        }
        
        return new NodeStatsIterator(first);
    }


    private class NodeStatsIterator implements Iterator<NodeStats>
    {

        private NodeStats current;
        
        
        public NodeStatsIterator(NodeStats first)
        {
            super();
            current = first;
        }
        
        public boolean hasNext()
        {
            return current != null;
        }

        public NodeStats next()
        {
            if (!hasNext())
                throw new NoSuchElementException();
            
            NodeStats stats = current;
            current = current.getNext();
            return stats;
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }

    }

}
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

import java.io.IOException;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.annotate.JsonDeserialize;

/**
 * The encapsulation of the data returned by the Riak <code>/stats</code> 
 * operation.
 * <p>
 * By implementing the {@link Iterable<NodeStats>} interface it contains N sets
 * of data where N is the number of connections the current client holds.
 * </p><p>
 * Worth noting is that integer values are returned as {@link BigInteger} objects. 
 * This is done to match the unconstrained size provided by Riak. 
 * </p><p>
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

// We set this so as not to break is new stats are added to the Riak /stats operation


@JsonIgnoreProperties(ignoreUnknown=true) 
public class NodeStats implements Iterable<NodeStats>
{
    
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
    
    @JsonProperty private BigInteger vnode_gets;
    @JsonProperty private BigInteger vnode_puts;
    @JsonProperty private BigInteger vnode_index_reads;
    @JsonProperty private BigInteger vnode_index_writes;
    @JsonProperty private BigInteger vnode_index_writes_postings;
    @JsonProperty private BigInteger vnode_index_deletes;
    @JsonProperty private BigInteger vnode_index_deletes_postings;
    @JsonProperty private BigInteger read_repairs;
    @JsonProperty private BigInteger vnode_gets_total;
    @JsonProperty private BigInteger vnode_puts_total;
    @JsonProperty private BigInteger vnode_index_reads_total;
    @JsonProperty private BigInteger vnode_index_writes_total;
    @JsonProperty private BigInteger vnode_index_writes_postings_total;
    @JsonProperty private BigInteger vnode_index_deletes_total;
    @JsonProperty private BigInteger vnode_index_deletes_postings_total;
    @JsonProperty private BigInteger node_gets;
    @JsonProperty private BigInteger node_gets_total;
    @JsonProperty private BigInteger node_get_fsm_time_mean;
    @JsonProperty private BigInteger node_get_fsm_time_median;
    @JsonProperty private BigInteger node_get_fsm_time_95;
    @JsonProperty private BigInteger node_get_fsm_time_99;
    @JsonProperty private BigInteger node_get_fsm_time_100;
    @JsonProperty private BigInteger node_puts;
    @JsonProperty private BigInteger node_puts_total;
    @JsonProperty private BigInteger node_put_fsm_time_mean;
    @JsonProperty private BigInteger node_put_fsm_time_median;
    @JsonProperty private BigInteger node_put_fsm_time_95;
    @JsonProperty private BigInteger node_put_fsm_time_99;
    @JsonProperty private BigInteger node_put_fsm_time_100;
    @JsonProperty private BigInteger node_get_fsm_siblings_mean;
    @JsonProperty private BigInteger node_get_fsm_siblings_median;
    @JsonProperty private BigInteger node_get_fsm_siblings_95;
    @JsonProperty private BigInteger node_get_fsm_siblings_99;
    @JsonProperty private BigInteger node_get_fsm_siblings_100;
    @JsonProperty private BigInteger node_get_fsm_objsize_mean;
    @JsonProperty private BigInteger node_get_fsm_objsize_median;
    @JsonProperty private BigInteger node_get_fsm_objsize_95;
    @JsonProperty private BigInteger node_get_fsm_objsize_99;
    @JsonProperty private BigInteger node_get_fsm_objsize_100;
    @JsonProperty private BigInteger read_repairs_total;
    @JsonProperty private BigInteger coord_redirs_total;
    @JsonProperty private BigInteger precommit_fail;            // Riak 1.1
    @JsonProperty private BigInteger postcommit_fail;           // Riak 1.1
    @JsonProperty private BigInteger cpu_nprocs;
    @JsonProperty private BigInteger cpu_avg1;
    @JsonProperty private BigInteger cpu_avg5;
    @JsonProperty private BigInteger cpu_avg15;
    @JsonProperty private BigInteger mem_total;
    @JsonProperty private BigInteger mem_allocated;
    @JsonProperty private String nodename;
    @JsonProperty private String[] connected_nodes;
    @JsonProperty private String sys_driver_version;
    @JsonProperty private BigInteger sys_global_heaps_size;
    @JsonProperty private String sys_heap_type;
    @JsonProperty private BigInteger sys_logical_processors;
    @JsonProperty private String sys_otp_release;
    @JsonProperty private BigInteger sys_process_count;
    @JsonProperty private boolean sys_smp_support;
    @JsonProperty private String sys_system_version;
    @JsonProperty private String sys_system_architecture;
    @JsonProperty private boolean sys_threads_enabled;
    @JsonProperty private BigInteger sys_thread_pool_size;
    @JsonProperty private BigInteger sys_wordsize;
    @JsonProperty private String[] ring_members;
    @JsonProperty private BigInteger ring_num_partitions;
    @JsonProperty private String ring_ownership;
    @JsonProperty private BigInteger ring_creation_size;
    @JsonProperty private String storage_backend;
    @JsonProperty private BigInteger pbc_connects_total;
    @JsonProperty private BigInteger pbc_connects;
    @JsonProperty private BigInteger pbc_active;
    @JsonProperty private String ssl_version;
    @JsonProperty private String public_key_version;
    @JsonProperty private String runtime_tools_version;
    @JsonProperty private String basho_stats_version;
    @JsonProperty private String riak_search_version;
    @JsonProperty private String merge_index_version;
    @JsonProperty private String luwak_version;
    @JsonProperty private String skerl_version;
    @JsonProperty private String riak_kv_version;
    @JsonProperty private String bitcask_version;
    @JsonProperty private String luke_version;
    @JsonProperty private String erlang_js_version;
    @JsonProperty private String mochiweb_version;
    @JsonProperty private String inets_version;
    @JsonProperty private String riak_pipe_version;
    @JsonProperty private String riak_core_version;
    @JsonProperty private String riak_sysmon_version;
    @JsonProperty private String webmachine_version;
    @JsonProperty private String crypto_version;
    @JsonProperty private String os_mon_version;
    @JsonProperty private String cluster_info_version;
    @JsonProperty private String sasl_version;
    @JsonProperty private String lager_version;
    @JsonProperty private String basho_metrics_version;
    @JsonProperty private String riak_control_version;          // Riak 1.1
    @JsonProperty private String stdlib_version;
    @JsonProperty private String kernel_version;
    @JsonProperty private BigInteger executing_mappers;
    @JsonProperty private BigInteger memory_total;
    @JsonProperty private BigInteger memory_processes;
    @JsonProperty private BigInteger memory_processes_used;
    @JsonProperty private BigInteger memory_system;
    @JsonProperty private BigInteger memory_atom;
    @JsonProperty private BigInteger memory_atom_used;
    @JsonProperty private BigInteger memory_binary;
    @JsonProperty private BigInteger memory_code;
    @JsonProperty private BigInteger memory_ets;
    @JsonProperty private BigInteger ignored_gossip_total;      // Riak 1.1
    @JsonProperty private BigInteger rings_reconciled_total;    // Riak 1.1
    @JsonProperty private BigInteger rings_reconciled;          // Riak 1.1
    @JsonProperty private BigInteger gossip_received;           // Riak 1.1
    @JsonDeserialize(using=UndefinedStatDeserializer.class)
    @JsonProperty private BigInteger converge_delay_min;        // Riak 1.1
    @JsonProperty private BigInteger converge_delay_max;        // Riak 1.1
    @JsonProperty private BigInteger converge_delay_mean;       // Riak 1.1
    @JsonDeserialize(using=UndefinedStatDeserializer.class)
    @JsonProperty private BigInteger converge_delay_last;       // Riak 1.1
    @JsonDeserialize(using=UndefinedStatDeserializer.class)
    @JsonProperty private BigInteger rebalance_delay_min;       // Riak 1.1
    @JsonProperty private BigInteger rebalance_delay_max;       // Riak 1.1
    @JsonProperty private BigInteger rebalance_delay_mean;      // Riak 1.1 
    @JsonDeserialize(using=UndefinedStatDeserializer.class)
    @JsonProperty private BigInteger rebalance_delay_last;      // Riak 1.1
    @JsonProperty private BigInteger riak_kv_vnodes_running;    // Riak 1.1
    @JsonProperty private BigInteger riak_kv_vnodeq_min;        // Riak 1.1
    @JsonProperty private BigInteger riak_kv_vnodeq_median;     // Riak 1.1
    @JsonProperty private BigInteger riak_kv_vnodeq_mean;       // Riak 1.1
    @JsonProperty private BigInteger riak_kv_vnodeq_max;        // Riak 1.1
    @JsonProperty private BigInteger riak_kv_vnodeq_total;      // Riak 1.1
    @JsonProperty private BigInteger riak_pipe_vnodes_running;  // Riak 1.1
    @JsonProperty private BigInteger riak_pipe_vnodeq_min;      // Riak 1.1
    @JsonProperty private BigInteger riak_pipe_vnodeq_median;   // Riak 1.1
    @JsonProperty private BigInteger riak_pipe_vnodeq_mean;     // Riak 1.1
    @JsonProperty private BigInteger riak_pipe_vnodeq_max;      // Riak 1.1
    @JsonProperty private BigInteger riak_pipe_vnodeq_total;    // Riak 1.1
    
    
    /**
     * Returns the <code>vnode_gets</code> value from the Riak stats reply
     * @return int value
     */
    public BigInteger vnodeGets() 
    {
        return vnode_gets;
    }
 
    /**
     * Returns the <code>vnode_gets</code> value from the Riak stats reply
     * @return <code>int</code> value
     */
    public BigInteger vnodePuts()
    {
        return vnode_puts;
    }
    
    /**
     * Returns the <code>vnode_index_reads</code> value from the Riak stats reply
     * @return <code>int</code> value
     */
    public BigInteger vnodeIndexReads()
    {
        return vnode_index_reads; 
    }
    
    /**
     * Returns the <code>vnode_index_writes</code> value from the Riak stats reply
     * @return <code>int</code> value
     */
    public BigInteger vnodeIndexWrites()
    {
        return vnode_index_writes;
    }
    
    /**
     * Returns the <code>vnode_index_writes_postings</code> value from the Riak stats reply
     * @return <code>int</code> value
     */
    public BigInteger vnodeIndexWritePostings()
    {
        return vnode_index_writes_postings;
    }
    
    /**
     * Returns the <code>vnode_index_deletes</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */     
    public BigInteger vnodeIndexDeletes()
    {
        return vnode_index_deletes;
    }
    
    /**
     * Returns the <code>vnode_index_deletes_postings</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */     
    public BigInteger vnodesIndexDeletesPostings()
    {
        return vnode_index_deletes_postings;
    }
    
    /**
     * Returns the <code>read_repairs</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */     
    public BigInteger readRepairs()
    {
        return read_repairs;
    }
    
    /**
     * Returns the <code>vnode_gets_total</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */     
    public BigInteger vnodeGetsTotal()
    {
        return vnode_gets_total;
    }
    
    /**
     * Returns the <code>vnode_puts_total</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */     
    public BigInteger vnodePutsTotal()
    {
        return vnode_puts_total;
    }
    
    /**
     * Returns the <code>vnode_index_reads_total</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */     
    public BigInteger vnodeIndexReadsTotal()
    {
        return vnode_index_reads_total;
    }
    
    /**
     * Returns the <code>vnode_index_writes_total</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */     
    public BigInteger vnodeIndexWritesTotal()
    {
        return vnode_index_writes_total;
    }
    
    /**
     * Returns the <code>vnode_index_writes_postings_total</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */     
    public BigInteger vnodeIndexWritesPostingsTotal()
    {
        return vnode_index_writes_postings_total;
    }
    
    /**
     * Returns the <code>vnode_index_deletes_total</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */     
    public BigInteger vnodeIndexDeletesTotal()
    {
        return vnode_index_deletes_total;
    }
    
    /**
     * Returns the <code>vnode_index_deletes_postings_total</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */     
    public BigInteger vnodeIndexDeletesPostingsTotal()
    {
        return vnode_index_deletes_postings_total;
    }
    
    /**
     * Returns the <code>node_gets</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */     
    public BigInteger nodeGets()
    {
        return node_gets;
    }
    
    /**
     * Returns the <code>node_gets_total</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */     
    public BigInteger nodeGetsTotal()
    {
        return node_gets_total;
    }
    
    /**
     * Returns the <code>node_get_fsm_time_mean</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */     
    public BigInteger nodeGetFsmTimeMean()
    {
        return node_get_fsm_time_mean;
    }
    
    /**
     * Returns the <code>node_get_fsm_time_median</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */     
    public BigInteger nodeGetFsmTimeMedian()
    {
        return node_get_fsm_time_median;
    }
    
    /**
     * Returns the <code>node_get_fsm_time_95</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */     
    public BigInteger nodeGetFsmTime95()
    {
        return node_get_fsm_time_95;
    }
    
    /**
     * Returns the <code>node_get_fsm_time_99</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */     
    public BigInteger nodeGetFsmTime99()
    {
        return node_get_fsm_time_99;
    }
    
    /**
     * Returns the <code>node_get_fsm_time_100</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */     
    public BigInteger nodeGetFsmTime100()
    {
        return node_get_fsm_time_100;
    }
    
    /**
     * Returns the <code>node_puts</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */     
    public BigInteger nodePuts()
    {
        return node_puts;
    }
    
    /**
     * Returns the <code>node_puts_total</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */     
    public BigInteger nodePutsTotal()
    {
        return node_puts_total;
    }
    
    /**
     * Returns the <code>node_get_fsm_time_mean</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */     
    public BigInteger nodePutFsmTimeMean()
    {
        return node_put_fsm_time_mean;
    }
    
    /**
     * Returns the <code>node_put_fsm_time_median</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */     
    public BigInteger nodePutFsmTimeMedian()
    {
        return node_put_fsm_time_median;
    }
    
    /**
     * Returns the <code>node_put_fsm_time_95</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */     
    public BigInteger nodePutFsmTime95()
    {
        return node_put_fsm_time_95;
    }
    
    /**
     * Returns the <code>node_put_fsm_time_99</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */     
    public BigInteger nodePutFsmTime99()
    {
        return node_put_fsm_time_99;
    }
    
    /**
     * Returns the <code>node_put_fsm_time_100</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */     
    public BigInteger nodePutFsmTime100()
    {
        return node_put_fsm_time_100;
    }
    
    /**
     * Returns the <code>node_get_fsm_siblings_mean</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */     
    public BigInteger nodeGetFsmSiblingsMean()
    {
        return node_get_fsm_siblings_mean;
    }
    
    /**
     * Returns the <code>node_put_fsm_siblings_median</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */     
    public BigInteger nodeGetFsmSiblingsMedian()
    {
        return node_get_fsm_siblings_median;
    }
    
    /**
     * Returns the <code>node_put_fsm_siblings_95</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */     
    public BigInteger nodeGetFsmSiblings95()
    {
        return node_get_fsm_siblings_95;
    }
    
    /**
     * Returns the <code>node_put_fsm_siblings_99</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */     
    public BigInteger nodeGetFsmSiblings99()
    {
        return node_get_fsm_siblings_99;
    }
    
    /**
     * Returns the <code>node_put_fsm_siblings_100</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */     
    public BigInteger nodeGetFsmSiblings100()
    {
        return node_get_fsm_siblings_100;
    }
    
    /**
     * Returns the <code>node_get_fsm_objsize_mean</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger nodeGetFsmObjsizeMean()
    {
        return node_get_fsm_objsize_mean;
    }
    
    /**
     * Returns the <code>node_put_fsm_objsize_median</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger nodeGetFsmObjsizeMedian()
    {
        return node_get_fsm_objsize_median;
    }
    
    /**
     * Returns the <code>node_put_fsm_objsize_95</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger nodeGetFsmObjsize95()
    {
        return node_get_fsm_objsize_95;
    }
    
    /**
     * Returns the <code>node_put_fsm_objsize_99</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger nodeGetFsmObjsize99()
    {
        return node_get_fsm_objsize_99;
    }
    
    /**
     * Returns the <code>node_put_fsm_objsize_100</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger nodePutFsmSObjsize100()
    {
        return node_get_fsm_objsize_100;
    }
    
    /**
     * Returns the <code>read_repairs_total</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger readRepairsTotal()
    {
        return read_repairs_total;
    }
    
    /**
     * Returns the <code>coord_redirs_total</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger coordRedirsTotal()
    {
        return coord_redirs_total;
    }
    
    /**
     * Returns the <code>cpu_nprocs</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger cpuNumProcs()
    {
        return cpu_nprocs;
    }
    
    /**
     * Returns the <code>cpu_avg1</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger cpuAvg1()
    {
        return cpu_avg1;
    }
    
    /**
     * Returns the <code>cpu_avg5</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger cpuAvg5()
    {
        return cpu_avg5;
    }

    /**
     * Returns the <code>cpu_avg15</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger cpuAvg15()
    {
        return cpu_avg15;
    }
    
    /**
     * Returns the <code>mem_total</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger memTotal()
    {
        return mem_total;
    }
    
    /**
     * Returns the <code>mem_allocated</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger memAllocated()
    {
        return mem_allocated;
    }
    
    /**
     * Returns the <code>nodename</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String nodename()
    {
        return nodename;
    }
    
    /**
     * Returns the <code>connected_nodes</code> value from the Riak Stats reply
     * @return <code>String[]</code> of node names
     */
    public String[] connectedNodes()
    {
        return connected_nodes;
    }
    
    /**
     * Returns the <code>sys_driver_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String sysDriverVersion()
    {
        return sys_driver_version;
    }
    
    /**
     * Returns the <code>sys_global_heaps_size</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger sysGlobalHeapsSize()
    {
        return sys_global_heaps_size;
    }
    
    /**
     * Returns the <code>sys_heap_type</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String sysHeapType()
    {
        return sys_heap_type;
    }
    
    /**
     * Returns the <code>sys_logical_processors</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger sysLogicalProcessors()
    {
        return sys_logical_processors;
    }
    
    /**
     * Returns the <code>sys_otp_release</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String sysOtpRelease()
    {
        return sys_otp_release;
    }
    
    /**
     * Returns the <code>sys_process_count</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger sysProcessCount()
    {
        return sys_process_count;
    }
    
    /**
     * Returns the <code>sys_smp_support</code> value from the Riak Stats reply
     * @return <code>boolean</code> value
     */
    public boolean sysSmpSupport()
    {
        return sys_smp_support;
    }
    
    /**
     * Returns the <code>sys_system_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String sysSystemVersion()
    {
        return sys_system_version;
    }
    
    /**
     * Returns the <code>sys_system_architecture</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String sysSystemArchitecture()
    {
        return sys_system_architecture;
    }
    
    /**
     * Returns the <code>sys_threads_enabled</code> value from the Riak Stats reply
     * @return <code>boolean</code> value
     */
    public boolean sysThreadsEnabled()
    {
        return sys_threads_enabled;
    }
    
    /**
     * Returns the <code>sys_thread_pool_size</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger sysThreadPoolSize()
    {
        return sys_thread_pool_size;
    }
    
    /**
     * Returns the <code>sys_wordsize</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger sysWordSize()
    {
        return sys_wordsize;
    }
    
    /**
     * Returns the <code>ring_members</code> value from the Riak Stats reply
     * @return <code>String[]</code> of node names
     */
    public String[] ringMembers()
    {
        return ring_members;
    }
    
    /**
     * Returns the <code>ring_num_partitions</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger ringNumPartitions()
    {
        return ring_num_partitions;
    }
    
    /**
     * Returns the <code>ring_ownership</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String ringOwnership()
    {
        return ring_ownership;
    }
    
    /**
     * Returns the <code>ring_creation_size</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger ringCreationSize()
    {
        return ring_creation_size;
    }
    
    /**
     * Returns the <code>storage_backend</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String storageBackend()
    {
        return storage_backend;
    }
    
    /**
     * Returns the <code>pbc_connects_total</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger pbcConnectsTotal()
    {
        return pbc_connects_total;
    }
    
    /**
     * Returns the <code>pbc_connects</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger pbcConnects()
    {
        return pbc_connects;
    }
    
    /**
     * Returns the <code>pbc_active</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger pbcActive()
    {
        return pbc_active;
    }
    
    /**
     * Returns the <code>ssl_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String sslVeriosn()
    {
        return ssl_version;
    }
    
    /**
     * Returns the <code>public_key_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String publicKeyVersion()
    {
        return public_key_version;
    }
    
    /**
     * Returns the <code>runtime_tools_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String runtimeToolsVersion()
    {
        return runtime_tools_version;
    }
    
    /**
     * Returns the <code>basho_stats_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String bashoStatsVersion()
    {
        return basho_stats_version;
    }
    
    /**
     * Returns the <code>riak_search_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String riakSearchVersion()
    {
        return riak_search_version;
    }
    
    /**
     * Returns the <code>merge_index_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String mergeIndexVersion()
    {
        return merge_index_version;
    }
    
    /**
     * Returns the <code>luwak_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String luwakVersion()
    {
        return luwak_version;
    }
    
    /**
     * Returns the <code>skerl_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String skerlVersion()
    {
        return skerl_version;
    }
    
    /**
     * Returns the <code>riak_kv_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String riakKvVersion()
    {
        return riak_kv_version;
    }
    
    /**
     * Returns the <code>bitcask_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String bitcaskVersion()
    {
        return bitcask_version;
    }
    
    /**
     * Returns the <code>luke_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String lukeVeriosn()
    {
        return luke_version;
    }
    
    /**
     * Returns the <code>erlang_js_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String erlangJsVersion()
    {
        return erlang_js_version;
    }
    
    /**
     * Returns the <code>mochiweb_value</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String mochiwebVersion()
    {
        return mochiweb_version;
    }
    
    /**
     * Returns the <code>inets_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String inetsVersion()
    {
        return inets_version;
    }
    
    /**
     * Returns the <code>riak_pipe_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String riakPipeVersion()
    {
        return riak_pipe_version;
    }
    
    /**
     * Returns the <code>riak_core_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String riakCoreVersion()
    {
        return riak_core_version;
    }
    
    /**
     * Returns the <code>riak_sysmon_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String riak_sysmon_version()
    {
        return riak_sysmon_version;
    }
    
    /**
     * Returns the <code>webmachine_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String webmachineVersion()
    {
        return webmachine_version;
    }
    
    /**
     * Returns the <code>crypto_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String cryptoVersion()
    {
        return crypto_version;
    }
    
    /**
     * Returns the <code>os_mon_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String osMonVersion()
    {
        return os_mon_version;
    }
    
    /**
     * Returns the <code>cluster_info_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String clusterInfoVersion()
    {
        return cluster_info_version;
    }
    
    /**
     * Returns the <code>sasl_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String saslVersion()
    {
        return sasl_version;
    }
    
    /**
     * Returns the <code>lager_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String lagerVersion()
    {
        return lager_version;
    }
    
    /**
     * Returns the <code>basho_lager_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String bashoMetricsVersion()
    {
        return basho_metrics_version;
    }
    
    /**
     * Returns the <code>stdlib_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String stdlibVersion()
    {
        return stdlib_version;
    }
    
    /**
     * Returns the <code>kernel_version</code> value from the Riak stats reply
     * @return <code>String</code> value
     */
    public String kernelVersion()
    {
        return kernel_version;
    }
    
    /**
     * Returns the <code>executing_mappers</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger executingMappers()
    {
        return executing_mappers;
    }
    
    /**
     * Returns the <code>memory_total</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger memoryTotal()
    {
        return memory_total;
    }
    
    /**
     * Returns the <code>memory_processes</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger memoryProcesses()
    {
        return memory_processes;
    }
    
    /**
     * Returns the <code>memory_processes_used</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger memoryProcessesUsed()
    {
        return memory_processes_used;
    }
    
    /**
     * Returns the <code>memory_system</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger memorySystem()
    {
        return memory_system;
    }
    
    /**
     * Returns the <code>memory_atom</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger memoryAtom()
    {
        return memory_atom;
    }
    
    /**
     * Returns the <code>memory_atom_used</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger memoryAtomUsed()
    {
        return memory_atom_used;
    }
    
    /**
     * Returns the <code>memory_binary</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger memoryBinary()
    {
        return memory_binary;
    }
    
    /**
     * Returns the <code>memory_code</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger memoryCode()
    {
        return memory_code;
    }
    
    /**
     * Returns the <code>memory_ets</code> value from the Riak Stats reply
     * @return <code>int</code> value
     */
    public BigInteger memoryEts()
    {
        return memory_ets;
    }

    /**
     * @return the precommit_fail
     */
    public BigInteger precommitFail()
    {
        return precommit_fail;
    }

    /**
     * @return the postcommit_fail
     */
    public BigInteger postcommitFail()
    {
        return postcommit_fail;
    }

    /**
     * @return the ignored_gossip_total
     */
    public BigInteger ignoredGossipTotal()
    {
        return ignored_gossip_total;
    }

    /**
     * @return the rings_reconciled_total
     */
    public BigInteger ringsReconciledTotal()
    {
        return rings_reconciled_total;
    }

    /**
     * @return the rings_reconciled
     */
    public BigInteger ringsReconciled()
    {
        return rings_reconciled;
    }

    /**
     * @return the gossip_received
     */
    public BigInteger gossipReceived()
    {
        return gossip_received;
    }

    /**
     * @return the converge_delay_min
     */
    public BigInteger convergeDelayMin()
    {
        return converge_delay_min;
    }

    /**
     * @return the converge_delay_max
     */
    public BigInteger convergeDelayMax()
    {
        return converge_delay_max;
    }

    /**
     * @return the converge_delay_mean
     */
    public BigInteger convergeDelayMean()
    {
        return converge_delay_mean;
    }

    /**
     * @return the converge_delay_last
     */
    public BigInteger convergeDelayLast()
    {
        return converge_delay_last;
    }

    /**
     * @return the rebalance_delay_min
     */
    public BigInteger rebalanceDelayMin()
    {
        return rebalance_delay_min;
    }

    /**
     * @return the rebalance_delay_max
     */
    public BigInteger rebalanceDelayMax()
    {
        return rebalance_delay_max;
    }

    /**
     * @return the rebalance_delay_mean
     */
    public BigInteger rebalanceDelayMean()
    {
        return rebalance_delay_mean;
    }

    /**
     * @return the rebalanceDelay_last
     */
    public BigInteger rebalanceDelayLast()
    {
        return rebalance_delay_last;
    }

    /**
     * @return the riak_kv_vnodes_running
     */
    public BigInteger riakKvVnodesRunning()
    {
        return riak_kv_vnodes_running;
    }

    /**
     * @return the riak_kv_vnodeq_min
     */
    public BigInteger riakKvVnodeqMin()
    {
        return riak_kv_vnodeq_min;
    }

    /**
     * @return the riak_kv_vnodeq_median
     */
    public BigInteger riakKvVnodeqMedian()
    {
        return riak_kv_vnodeq_median;
    }

    /**
     * @return the riak_kv_vnodeq_mean
     */
    public BigInteger riakKvVnodeqMean()
    {
        return riak_kv_vnodeq_mean;
    }

    /**
     * @return the riak_kv_vnodeq_max
     */
    public BigInteger riakKvVnodeqMax()
    {
        return riak_kv_vnodeq_max;
    }

    /**
     * @return the riak_kv_vnodeq_total
     */
    public BigInteger riakKvVnodeqTotal()
    {
        return riak_kv_vnodeq_total;
    }

    /**
     * @return the riak_pipe_vnodes_running
     */
    public BigInteger riakPipeVnodesRunning()
    {
        return riak_pipe_vnodes_running;
    }

    /**
     * @return the riak_pipe_vnodeq_min
     */
    public BigInteger riakPipeVnodeqMin()
    {
        return riak_pipe_vnodeq_min;
    }

    /**
     * @return the riak_pipe_vnodeq_median
     */
    public BigInteger riakPipeVnodeqMedian()
    {
        return riak_pipe_vnodeq_median;
    }

    /**
     * @return the riak_pipe_vnodeq_mean
     */
    public BigInteger riakPipeVnodeqMean()
    {
        return riak_pipe_vnodeq_mean;
    }

    /**
     * @return the riak_pipe_vnodeq_max
     */
    public BigInteger riakPipeVnodeqMax()
    {
        return riak_pipe_vnodeq_max;
    }

    /**
     * @return the riak_pipe_vnodeq_total
     */
    public BigInteger riakPipeVnodeqTotal()
    {
        return riak_pipe_vnodeq_total;
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
/* There's a few stats that are currently not being properly serialized
*  to JSON by Riak that have a string value ("undefined") instead of 0 (or
* any integer value). This fixes those. 
*/
class UndefinedStatDeserializer extends JsonDeserializer<BigInteger>
{
    @Override
    public BigInteger deserialize(JsonParser jp, DeserializationContext dc) throws IOException, JsonProcessingException
    {
        if (jp.getCurrentToken() == JsonToken.VALUE_STRING)
        {
            return BigInteger.valueOf(0L);
        }
        else
            return BigInteger.valueOf(jp.getLongValue());
        
    }

}
    
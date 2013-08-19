package com.basho.riak.client;

import com.basho.riak.client.cap.DefaultResolver;
import com.basho.riak.client.convert.PassThroughConverter;
import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.operations.FetchOperation;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.util.ByteArrayWrapper;
import com.google.protobuf.ByteString;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

/**
 * Technology preview
 * 
 * @author Brian Roach <roach at basho dot com>
 */
public class App 
{
    public static void main( String[] args ) throws UnknownHostException, InterruptedException, ExecutionException
    {
        RiakNode.Builder builder = new RiakNode.Builder()
                                        .withMinConnections(10);
        
        RiakCluster cluster = new RiakCluster.Builder(builder.build()).build();
        cluster.start();
        
        FutureOperation<RiakObject> fetchOp = 
            new FetchOperation<RiakObject>(ByteArrayWrapper.unsafeCreate("test_bucket".getBytes()), ByteArrayWrapper.unsafeCreate("test_key2".getBytes()))
                    .withConverter(new PassThroughConverter())
                    .withResolver(new DefaultResolver<RiakObject>());
        
        cluster.execute(fetchOp);
        
        RiakObject ro = fetchOp.get();
        
        System.out.println("value: " + ro.getValue());
        System.out.println(ro.isDeleted());
        System.out.println(ro.isNotFound());
        
        cluster.stop();
        
        
        
    }
}

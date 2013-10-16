package com.basho.riak.client;

import com.basho.riak.client.cap.DefaultResolver;
import com.basho.riak.client.convert.PassThroughConverter;
import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.RiakFutureListener;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.operations.FetchBucketPropsOperation;
import com.basho.riak.client.core.operations.FetchBucketTypePropsOperation;
import com.basho.riak.client.core.operations.FetchOperation;
import com.basho.riak.client.query.BucketProperties;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.util.ByteArrayWrapper;
import com.basho.riak.protobuf.RiakKvPB;
import com.basho.riak.protobuf.RiakPB;
import com.google.protobuf.ByteString;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Technology preview
 * 
 * @author Brian Roach <roach at basho dot com>
 */
public class App implements RiakFutureListener<RiakObject>
{
    private final RiakCluster cluster;
    
    public App() throws Exception
    {
        RiakNode.Builder builder = new RiakNode.Builder()
                                        .withMinConnections(10);
        
        cluster = new RiakCluster.Builder(builder.build()).build();
        cluster.start();
        Thread.sleep(3000);
    }
    
    public void doIt() throws InterruptedException, ExecutionException
    {
        FetchBucketTypePropsOperation btpOp = 
            new FetchBucketTypePropsOperation(ByteArrayWrapper.unsafeCreate("test_type2".getBytes()));
        
        cluster.execute(btpOp);
        BucketProperties props = btpOp.get();
        System.out.println(props);
        
        FetchBucketPropsOperation bpOp = 
            new FetchBucketPropsOperation(ByteArrayWrapper.unsafeCreate("test_bucket3)".getBytes()))
                .withBucketType(ByteArrayWrapper.unsafeCreate("test_type2".getBytes()));
                               
        cluster.execute(bpOp);
        props = bpOp.get();
        System.out.println(props);
        
        
        
        
        FutureOperation<RiakObject, RiakKvPB.RpbGetResp> fetchOp =
            new FetchOperation<RiakObject>(ByteArrayWrapper.unsafeCreate("test_bucket2".getBytes()), ByteArrayWrapper.unsafeCreate("test_key2".getBytes()))
                    .withConverter(new PassThroughConverter())
                    .withResolver(new DefaultResolver<RiakObject>());
        
        //fetchOp.addListener(this);
        cluster.execute(fetchOp);
        RiakObject ro =  fetchOp.get();
        System.out.println("value: " + ro.getValue());
            System.out.println(ro.isDeleted());
            System.out.println(ro.isNotFound());
        cluster.stop();
    }
    
    public static void main( String[] args ) throws Exception
    {
        App a = new App();
        a.doIt();
    }

    @Override
    public void handle(RiakFuture<RiakObject> f)
    {
        try
        {
            RiakObject ro = f.get();
            System.out.println("value: " + ro.getValue());
            System.out.println(ro.isDeleted());
            System.out.println(ro.isNotFound());
        
            cluster.stop();
        }
        catch (InterruptedException ex)
        {
            Logger.getLogger(App.class.getName()).log(Level.SEVERE, null, ex);
        }
        catch (ExecutionException ex)
        {
            Logger.getLogger(App.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}

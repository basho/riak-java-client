package com.basho.riak.client;

import com.basho.riak.client.cap.DefaultResolver;
import com.basho.riak.client.convert.PassThroughConverter;
import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.RiakFutureListener;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.operations.FetchOperation;
import com.basho.riak.client.query.RiakObject;
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
    }
    
    public void doIt() throws InterruptedException, ExecutionException
    {
        FutureOperation<RiakObject> fetchOp = 
            new FetchOperation<RiakObject>(ByteString.copyFromUtf8("test_bucket"), ByteString.copyFromUtf8("test_key2"))
                    .withConverter(new PassThroughConverter())
                    .withResolver(new DefaultResolver<RiakObject>());
        
        fetchOp.addListener(this);
        cluster.execute(fetchOp);
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

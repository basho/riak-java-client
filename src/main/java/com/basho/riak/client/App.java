package com.basho.riak.client;

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.RiakFutureListener;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.operations.FetchBucketPropsOperation;
import com.basho.riak.client.core.operations.FetchOperation;
import com.basho.riak.client.query.BucketProperties;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.RiakObject;

import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
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
        
        File pemFile = new File("/Users/roach/newRiakCerts/cacert.pem");
        FileInputStream in = new FileInputStream(pemFile);
        CertificateFactory cFactory = CertificateFactory.getInstance("X.509");
        X509Certificate caCert = (X509Certificate) cFactory.generateCertificate(in);
        in.close();
        
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null, "password".toCharArray());
        ks.setCertificateEntry("mycert", caCert);
        
        
        
        RiakNode.Builder builder = new RiakNode.Builder()
                                        .withMinConnections(1)
                                        .withAuth("roach", "changeme", ks);
                                        
        
        cluster = new RiakCluster.Builder(builder.build()).withExecutionAttempts(1).build();
        cluster.start();
        
        
        
    }
    
    public void doIt() throws InterruptedException, ExecutionException
    {

        try
        {

	        Location location = new Location("test_bucket3").setBucketType("test_type2");
	        FetchBucketPropsOperation bpOp =
		        new FetchBucketPropsOperation.Builder(location)
			        .build();

	        cluster.execute(bpOp);
	        BucketProperties props = bpOp.get().getBucketProperties();
	        System.out.println(props);


	        location = new Location("test_bucket2").setKey("test_key2");

	        FetchOperation fetchOp =
		        new FetchOperation.Builder(location)
			        .build();


	        //fetchOp.addListener(this);
	        cluster.execute(fetchOp);
	        FetchOperation.Response resp = fetchOp.get();
	        System.out.println(resp.isNotFound());
	        for (RiakObject ro : resp.getObjectList())
            {
                System.out.println("value: " + ro.getValue());
                System.out.println(ro.isDeleted());
            }

        }
        finally
        {
            cluster.shutdown();
        }
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
            
            cluster.shutdown();
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

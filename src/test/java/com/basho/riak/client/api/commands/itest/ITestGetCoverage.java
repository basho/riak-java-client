package com.basho.riak.client.api.commands.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.GetCoverage;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.operations.PingOperation;
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertTrue;

public class ITestGetCoverage extends ITestBase {
    private static Logger logger = LoggerFactory.getLogger(ITestGetCoverage.class);

    @Test(timeout = 60000)
    public void getFullCoverage() throws ExecutionException, InterruptedException, UnknownHostException {
        logger.info("Retrieve list of EPs...");
        final RiakClient client = new RiakClient(cluster);
        final GetCoverage cmd = new GetCoverage.Builder()
                .withForcedUpdate()
                .build();

        final GetCoverage.Response response = client.execute(cmd);

        logger.info("Got the following list of EPs: {}", response.entryPoints());

        /**
         * Since GetCoverage command is the only way to get an information about the number of available Riak nodes
         * there is no chance to verify count of returned EPs. Therefore we will iterate through the list of the
         * returned EPs and ping each of them to ensure that at least a valid EP was returned.
         * */
        verifyThatAllNodesAreAvailable(response.entryPoints());
    }

    @Test(timeout = 60000)
    public void getCoverageForLocation() throws ExecutionException, InterruptedException, UnknownHostException {
        final RiakClient client = new RiakClient(cluster);
        final String key = "coverage-4-location";

        // create test value
        final RiakObject ro = new RiakObject()
                .setValue(BinaryValue.create("{\"value\": 42}"))
                .setContentType("application/json");

        final StoreValue request = new StoreValue.Builder(ro)
                .withLocation(new Location(defaultNamespace(), key))
                .build();

        client.execute(request);

        // perform test
        final Location location = new Location(defaultNamespace(), key);
        logger.info("Retrieve list of EPs for location '{}' ...", location);

        final GetCoverage cmd = new GetCoverage.Builder()
                .withForcedUpdate()
                .withLocation(location)
                .build();

        final GetCoverage.Response response = client.execute(cmd);
        logger.info("Got the following list of EPs: {}", response.entryPoints());

        /**
         * Since GetCoverage command is the only way to get an information about the number of available Riak nodes
         * there is no chance to verify count of returned EPs. Therefore we will iterate through the list of the
         * returned EPs and ping each of them to ensure that at least a valid EP was returned.
         * */
        verifyThatAllNodesAreAvailable(response.entryPoints());
    }

    private static void verifyThatAllNodesAreAvailable(Collection<Map.Entry<String,Integer>> nodes) throws UnknownHostException, InterruptedException {
        for(Map.Entry<String,Integer> e: nodes){
            logger.info("Sending ping to EP {}:{}", e.getKey(), e.getValue());

            final RiakNode node = new RiakNode.Builder()
                    .withRemoteAddress(e.getKey())
                    .withRemotePort(e.getValue())
                    .withMinConnections(1)
                    .build()
                        .start();

            try{
                final PingOperation ping = new PingOperation();
                assertTrue(node.execute(ping));

                ping.await();

                assertTrue(ping.isSuccess());
                logger.info("EP Ping {}:{}: succeeded", e.getKey(), e.getValue());
            }finally{
                node.shutdown();
            }
        }
    }
}

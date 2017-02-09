package com.basho.riak.client.core.operations.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.indexes.IntIndexQuery;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.indexes.LongIntIndex;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class ITestInconsistent2iQueryPageableReads extends ITestBase {
    private static final Logger logger = LoggerFactory.getLogger(ITestInconsistent2iQueryPageableReads.class);
    private static Namespace DEFAULT_NAMESPACE = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, "2i-regression");
    private static final String INDEX_NAME = "creationIdx";

    @Before
    public void createTestData() throws ExecutionException, InterruptedException {

        // -- create test data
        final RiakClient c1 = new RiakClient(cluster);
        try{
            for(long l=1; l<=10; ++l){
                RiakObject ro = new RiakObject();
                ro.setValue(BinaryValue.create(Long.toString(l)));
                ro.getIndexes()
                    .getIndex(LongIntIndex.named(INDEX_NAME))
                    .add(l);


                StoreValue cmd = new StoreValue.Builder(ro)
                    .withLocation(new Location(DEFAULT_NAMESPACE, "k" + l))
                    .build();

                c1.execute(cmd);
            }
        }finally {
            //c1.shutdown();
        }

        // -- check creation
        final RiakClient c2 = new RiakClient(cluster);
        try {
            for (long l = 1; l <= 10; ++l) {
                boolean available = false;
                for(int retry=0; retry<10; ++retry) {
                    final IntIndexQuery q = new IntIndexQuery.Builder(DEFAULT_NAMESPACE, INDEX_NAME, l)
                            .build();

                    final IntIndexQuery.Response r = c2.execute(q);

                    available = r.hasEntries();
                    if( available ){
                        logger.info("Value [{}] was created: key: '{}'", l, r.getEntries().get(0).getRiakObjectLocation());
                        break;
                    }
                }

                if(!available){
                    throw new RuntimeException("Recently created test data is unavailable, creationIdx " + l);
                }
            }
        }finally {
            //c2.shutdown();
        }
    }


    private static List<Location> grabAllLocationsReturnedBy2iPaginatedQuery(Namespace namespace, String index,
                    long from, long to, int pageSize) throws ExecutionException, InterruptedException {

        final List<Location> results = new ArrayList(10);
        BinaryValue continuation = null;


        for(;;) {
            final RiakClient c = new RiakClient(cluster);
            try {

                final IntIndexQuery q = new IntIndexQuery.Builder(namespace, index, from, to)
                        .withMaxResults(pageSize)
                        .withContinuation(continuation)
                        .withPaginationSort(true)
                        .withKeyAndIndex(true)
                        .build();

                final IntIndexQuery.Response r = c.execute(q);

                final List<Location> locations;


                if(r.hasEntries()){
                    locations = new ArrayList<Location>(r.getEntries().size());

                    for(IntIndexQuery.Response.Entry e: r.getEntries()) {
                        locations.add(e.getRiakObjectLocation());
                    }

                    results.addAll(locations);
                }else{
                    locations = Collections.emptyList();
                }

                logger.debug("2i query(token={}) returns:\n  token: {}\n  locations: {}",
                    continuation, r.getContinuation(), locations);

                if (!r.hasContinuation() || !r.hasEntries()) {
                    break;
                }

                continuation = r.getContinuation();
            } finally {
                //c.shutdown();
            }
        }
        return results;
    }

    @Test
    public void test() throws ExecutionException, InterruptedException {
        for(int i=0; i<1000; ++i){
            logger.debug("Begin Iteration [{}]", i);
            final List<?> data = grabAllLocationsReturnedBy2iPaginatedQuery(DEFAULT_NAMESPACE, INDEX_NAME, 1L, 10L, 2);

            if(data.size() != 10){
                Assert.fail(String.format("Iteration [%s] - FAILED {Expected 10 items, but actually got %d", i, data.size()));
            }
            logger.info(String.format("Iteration [%s] - PASSED", i));
        }
    }
}

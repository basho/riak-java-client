package com.basho.riak.client.api.commands.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.BatchDelete;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.List;

public class ITestBatchDelete {

    public static class Book {
        public String title;
        public String author;
        public String body;
        public String isbn;
        public Integer copiesOwned;
    }

    // This will create a client object that we can use to interact with Riak
    private static RiakCluster setUpCluster() throws UnknownHostException {
        // This example will use only one node listening on localhost:10017
        RiakNode node = new RiakNode.Builder()
                .withRemoteAddress("127.0.0.1")
//                .withRemotePort(10017)
                .build();

        // This cluster object takes our one node as an argument
        RiakCluster cluster = new RiakCluster.Builder(node)
                .build();

        // The cluster must be started to work, otherwise you will see errors
        cluster.start();

        return cluster;
    }


    @Test
    public void testExecuteAsync() throws Exception {
        try {
            // First, we'll create a basic object storing a movie quote
            RiakObject quoteObject = new RiakObject()
                    // We tell Riak that we're storing plaintext, not JSON, HTML, etc.
                    .setContentType("text/plain")
                            // Objects are ultimately stored as binaries
                    .setValue(BinaryValue.create("You're dangerous, Maverick"));

            // In the new Java client, instead of buckets you interact with Namespace
            // objects, which consist of a bucket AND a bucket type; if you don't
            // supply a bucket type, "default" is used; the Namespace below will set
            // only a bucket, without supplying a bucket type
            Namespace quotesBucket = new Namespace("quotes");

            // With our Namespace object in hand, we can create a Location object,
            // which allows us to pass in a key as well
            Location quoteObjectLocation = new Location(quotesBucket, "Iceman");

            // With our RiakObject in hand, we can create a StoreValue operation
            StoreValue storeOp = new StoreValue.Builder(quoteObject)
                    .withLocation(quoteObjectLocation)
                    .build();

            // And now we can use our setUpCluster() function to create a cluster
            // object which we can then use to create a client object and then
            // execute our storage operation
            RiakCluster cluster = setUpCluster();
            RiakClient client = new RiakClient(cluster);

            client.execute(storeOp);

            // Now we can verify that the object has been stored properly by
            // creating and executing a FetchValue operation
            FetchValue fetchOp = new FetchValue.Builder(quoteObjectLocation)
                    .build();
            RiakObject fetchedObject = client.execute(fetchOp).getValue(RiakObject.class);
            assert (fetchedObject.getValue().equals(quoteObject.getValue()));

            // And we'll delete the object
            DeleteValue deleteOp = new DeleteValue.Builder(quoteObjectLocation)
                    .build();
            client.execute(deleteOp);

            Book mobyDick = new Book();
            mobyDick.title="Moby Dick";
            mobyDick.author="Herman Melville";
            mobyDick.body="Call me Ishmael. Some years ago...";
            mobyDick.isbn="1111979723";
            mobyDick.copiesOwned=3;

            Book mobyDick2 = new Book();
            mobyDick2.title="Moby Dick 2";
            mobyDick2.author="Herman Melville 2";
            mobyDick2.body="Call me Ishmael. Some years ago... 2";
            mobyDick2.isbn="1111979723 2";
            mobyDick2.copiesOwned=2;

            // Now we'll assign a Location for the book, create a StoreValue
            // operation, and store the book
            Namespace booksBucket = new Namespace("books");
            Location mobyDickLocation = new Location(booksBucket, "moby_dick");
            Location mobyDickLocation2 = new Location(booksBucket, "moby_dick_2");
            StoreValue storeBookOp = new StoreValue.Builder(mobyDick)
                    .withLocation(mobyDickLocation)
                    .build();
            StoreValue storeBookOp2 = new StoreValue.Builder(mobyDick2)
                    .withLocation(mobyDickLocation2)
                    .build();
            client.execute(storeBookOp);
            client.execute(storeBookOp2);

            // And we'll verify that we can fetch the info about Moby Dick and
            // that that info will match the object we created initially
            FetchValue fetchMobyDickOp = new FetchValue.Builder(mobyDickLocation)
                    .build();
            Book fetchedBook = client.execute(fetchMobyDickOp).getValue(Book.class);

            assert (mobyDick.getClass() == fetchedBook.getClass());
            assert (mobyDick.title.equals(fetchedBook.title));
            assert (mobyDick.author.equals(fetchedBook.author));
            // And so on...
            FetchValue fetchMobyDickOp2 = new FetchValue.Builder(mobyDickLocation2)
                    .build();
            Book fetchedBook2 = client.execute(fetchMobyDickOp2).getValue(Book.class);

            assert (mobyDick2.getClass() == fetchedBook2.getClass());
            assert (mobyDick2.title.equals(fetchedBook2.title));
            assert (mobyDick2.author.equals(fetchedBook2.author));

            BatchDelete.Builder batchDeleteBuilder = new BatchDelete.Builder();
            batchDeleteBuilder
                    .withTimeout(3000)
                    .addLocations(mobyDickLocation, mobyDickLocation2);
            BatchDelete batchDelete = batchDeleteBuilder.build();
            RiakFuture<BatchDelete.Response, List<Location>> future = client.executeAsync(batchDelete);
            future.await();

            Book deletedBook = client.execute(fetchMobyDickOp).getValue(Book.class);

            assert (deletedBook == null);

            Book deletedBook2 = client.execute(fetchMobyDickOp2).getValue(Book.class);

            assert (deletedBook2 == null);

            // Now that we're all finished, we should shut our cluster object down
            cluster.shutdown();

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }

}

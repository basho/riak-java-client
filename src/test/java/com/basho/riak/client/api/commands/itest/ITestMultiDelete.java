package com.basho.riak.client.api.commands.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.MultiDelete;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;

public class ITestMultiDelete extends ITestBase
{
    RiakClient client = new RiakClient(cluster);

    @Test
    public void testMultiDelete() throws ExecutionException, InterruptedException
    {
        // Insert Data
        Location[] bookLocations = insertBookData(client);

        // Verify Data was inserted
        FetchValue[] fetchCmds = verifyInsert(client, bookLocations);

        // Delete Data
        MultiDelete.Builder multiDeleteBuilder = new MultiDelete.Builder();
        multiDeleteBuilder.withTimeout(3000).addLocations(bookLocations);
        MultiDelete multiDelete = multiDeleteBuilder.build();
        RiakFuture<MultiDelete.Response, List<Location>> future = client.executeAsync(multiDelete);
        future.await(10, TimeUnit.SECONDS);

        // Verify Deleted Data
        verifyDeletedData(client, fetchCmds);
    }

    @Test
    public void testNoDeletes() throws InterruptedException, ExecutionException, TimeoutException
    {
        final MultiDelete multiDelete = new MultiDelete.Builder().build();
        final RiakFuture<MultiDelete.Response, List<Location>> future =
                client.executeAsync(multiDelete);

        final MultiDelete.Response deleteResponse = future.get(100, TimeUnit.MILLISECONDS);
        assertNotNull("Fail: Couldn't delete 0 records in 100ms", deleteResponse);
    }

    private Location[] insertBookData(RiakClient client)
            throws ExecutionException, InterruptedException
    {
        final Namespace booksBucket = new Namespace("books");
        Location[] bookLocations = new Location[] {
                new Location(booksBucket, "moby_dick"),
                new Location(booksBucket, "moby_dick_2")
        };

        Book mobyDick = new Book();
        mobyDick.title = "Moby Dick";
        mobyDick.author = "Herman Melville";
        mobyDick.body = "Call me Ishmael. Some years ago...";
        mobyDick.isbn = "1111979723";
        mobyDick.copiesOwned = 3;

        Book mobyDick2 = new Book();
        mobyDick2.title = "Moby Dick 2";
        mobyDick2.author = "Herman Melville 2";
        mobyDick2.body = "Call me Ishmael. Some years ago... 2";
        mobyDick2.isbn = "1111979723 2";
        mobyDick2.copiesOwned = 2;

        StoreValue storeBookOp = new StoreValue.Builder(mobyDick).withLocation(bookLocations[0]).build();
        StoreValue storeBookOp2 = new StoreValue.Builder(mobyDick2).withLocation(bookLocations[1]).build();
        client.execute(storeBookOp);
        client.execute(storeBookOp2);

        return bookLocations;
    }

    private FetchValue[] verifyInsert(RiakClient client, Location[] bookLocations)
            throws ExecutionException, InterruptedException
    {
        // And we'll verify that we can fetch the info about Moby Dick and
        // that that info will match the object we created initially
        FetchValue fetchMobyDickOp = new FetchValue.Builder(bookLocations[0]).build();
        Book fetchedBook = client.execute(fetchMobyDickOp).getValue(Book.class);
        assertNotNull(fetchedBook);

        FetchValue fetchMobyDickOp2 = new FetchValue.Builder(bookLocations[1]).build();
        Book fetchedBook2 = client.execute(fetchMobyDickOp2).getValue(Book.class);
        assertNotNull(fetchedBook2);

        return new FetchValue[] { fetchMobyDickOp, fetchMobyDickOp2 };
    }

    private void verifyDeletedData(RiakClient client, FetchValue[] fetchCmds)
            throws ExecutionException, InterruptedException
    {
        Book deletedBook = client.execute(fetchCmds[0]).getValue(Book.class);
        assertNull(deletedBook);

        Book deletedBook2 = client.execute(fetchCmds[1]).getValue(Book.class);
        assertNull(deletedBook2);
    }

    public static class Book
    {
        public String title;
        public String author;
        public String body;
        public String isbn;
        public Integer copiesOwned;
    }
}

package com.basho.riak.client.operations.mapreduce;

import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.operations.MapReduceOperation;
import com.basho.riak.client.operations.ImmediateRiakFuture;
import com.basho.riak.client.operations.Location;
import com.basho.riak.client.query.filter.EndsWithFilter;
import com.basho.riak.client.query.filter.GreaterThanFilter;
import com.basho.riak.client.query.functions.Function;
import com.basho.riak.client.util.BinaryValue;
import com.basho.riak.protobuf.RiakKvPB;
import junit.framework.TestCase;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;

public class MapReduceTest extends TestCase
{

	@Test
	@SuppressWarnings("unchecked")
	public void testBucketKeyMapReduce() throws ExecutionException, InterruptedException
	{

		RiakCluster cluster = Mockito.mock(RiakCluster.class);
		ArgumentCaptor<MapReduceOperation> operationCaptor =
			ArgumentCaptor.forClass(MapReduceOperation.class);
		Mockito.when(cluster.execute(operationCaptor.capture()))
			.thenReturn(new ImmediateRiakFuture<List<BinaryValue>>(Collections.EMPTY_LIST));

		BucketKeyMapReduce mapreduce = new BucketKeyMapReduce.Builder()
			.withLocation(new Location("bucket", "key"))
			.withMapPhase(Function.newErlangFunction("mod", "func"), true)
			.withMapPhase(Function.newAnonymousJsFunction("function() {}"), true)
			.withMapPhase(Function.newNamedJsFunction("my_func"), true)
			.withMapPhase(Function.newStoredJsFunction("map_bucket", "map_key"), true)
			.withReducePhase(Function.newErlangFunction("mod", "func"), true)
			.withReducePhase(Function.newAnonymousJsFunction("function() {}"), true)
			.withReducePhase(Function.newNamedJsFunction("my_func"), true)
			.withReducePhase(Function.newStoredJsFunction("map_bucket", "map_key"), true)
			.withLinkPhase("link_bucket", "link_tag")
			.build();

		mapreduce.execute(cluster);

		verify(cluster).execute(any(FutureOperation.class));

		MapReduceOperation operation = operationCaptor.getValue();

		RiakKvPB.RpbMapRedReq.Builder builder =
			(RiakKvPB.RpbMapRedReq.Builder) Whitebox.getInternalState(operation, "reqBuilder");


	}

	@Test
	@SuppressWarnings("unchecked")
	public void testBucketMapReduce() throws ExecutionException, InterruptedException
	{

		RiakCluster cluster = Mockito.mock(RiakCluster.class);
		ArgumentCaptor<MapReduceOperation> operationCaptor =
			ArgumentCaptor.forClass(MapReduceOperation.class);
		Mockito.when(cluster.execute(operationCaptor.capture()))
			.thenReturn(new ImmediateRiakFuture<List<BinaryValue>>(Collections.EMPTY_LIST));

		BucketMapReduce mapreduce = new BucketMapReduce.Builder()
			.withBucket(new Location("bucket"))
			.withKeyFilter(new EndsWithFilter("1"))
			.withKeyFilter(new GreaterThanFilter(100))
			.build();

		mapreduce.execute(cluster);

		verify(cluster).execute(any(FutureOperation.class));

		MapReduceOperation operation = operationCaptor.getValue();

		RiakKvPB.RpbMapRedReq.Builder builder =
			(RiakKvPB.RpbMapRedReq.Builder) Whitebox.getInternalState(operation, "reqBuilder");

	}

	@Test
	@SuppressWarnings("unchecked")
	public void testIndexMapReduce() throws ExecutionException, InterruptedException
	{

		RiakCluster cluster = Mockito.mock(RiakCluster.class);
		ArgumentCaptor<MapReduceOperation> operationCaptor =
			ArgumentCaptor.forClass(MapReduceOperation.class);
		Mockito.when(cluster.execute(operationCaptor.capture()))
			.thenReturn(new ImmediateRiakFuture<List<BinaryValue>>(Collections.EMPTY_LIST));

		IndexMapReduce mapreduce = new IndexMapReduce.Builder()
			.withBucket(new Location("bucket"))
			.withIndex("index")
			.withMatchValue(1)
			.build();

		mapreduce.execute(cluster);

		verify(cluster).execute(any(FutureOperation.class));

		MapReduceOperation operation = operationCaptor.getValue();

		RiakKvPB.RpbMapRedReq.Builder builder =
			(RiakKvPB.RpbMapRedReq.Builder) Whitebox.getInternalState(operation, "reqBuilder");

	}

	@Test
	@SuppressWarnings("unchecked")
	public void testSearchMapReduce() throws ExecutionException, InterruptedException
	{

		RiakCluster cluster = Mockito.mock(RiakCluster.class);
		ArgumentCaptor<MapReduceOperation> operationCaptor =
			ArgumentCaptor.forClass(MapReduceOperation.class);
		Mockito.when(cluster.execute(operationCaptor.capture()))
			.thenReturn(new ImmediateRiakFuture<List<BinaryValue>>(Collections.EMPTY_LIST));

		SearchMapReduce mapreduce = new SearchMapReduce.Builder()
			.withBucket(new Location("bucket"))
			.withQuery("query")
			.build();

		mapreduce.execute(cluster);

		verify(cluster).execute(any(FutureOperation.class));

		MapReduceOperation operation = operationCaptor.getValue();

		RiakKvPB.RpbMapRedReq.Builder builder =
			(RiakKvPB.RpbMapRedReq.Builder) Whitebox.getInternalState(operation, "reqBuilder");

	}


}

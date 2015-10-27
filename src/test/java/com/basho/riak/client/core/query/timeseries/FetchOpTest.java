package com.basho.riak.client.core.query.timeseries;

import com.basho.riak.client.core.operations.ts.FetchOperation;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;

/**
 * Created by alex on 10/26/15.
 */

public class FetchOpTest
{
    private final long now = 1443796900000l; // "now"
    private final String tableName = "my_table";
    private final Collection<Cell> keyValues = Arrays.asList(
            new Cell("my_family"), new Cell("my_series"), Cell.newTimestamp(now));

    @Test
    public void shouldBuildATsGetReqCorrectly()
    {
        FetchOperation cmd = new FetchOperation.Builder(tableName, keyValues).build();

    }

}

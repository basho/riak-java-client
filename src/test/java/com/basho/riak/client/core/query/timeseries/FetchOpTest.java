package com.basho.riak.client.core.query.timeseries;

import com.basho.riak.client.core.operations.ts.FetchOperation;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Time Series Query Operation Unit Tests
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */

public class FetchOpTest
{
    private final long now = 1443796900000L; // "now"
    private final String tableName = "my_table";
    private final List<Cell> keyValues = Arrays.asList(
            new Cell("my_family"), new Cell("my_series"), Cell.newTimestamp(now), null);

    @Test
    public void shouldBuildADescriptiveQueryInfoString()
    {
        String expectedInfo = "SELECT * FROM my_table WHERE PRIMARY KEY = { Cell{ my_family }, Cell{ my_series }, Cell{ 1443796900000 }, NULL }";
        FetchOperation cmd = new FetchOperation.Builder(tableName, keyValues).build();
        assertEquals(expectedInfo, cmd.getQueryInfo());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionIfTableNameIsMissing()
    {
        new FetchOperation.Builder(null, keyValues).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionIfTableNameIsBlank()
    {
        new FetchOperation.Builder("", keyValues).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionIfKeysAreNull()
    {
        new FetchOperation.Builder(tableName, null).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionIfKeysAreMissing()
    {
        new FetchOperation.Builder(tableName, new ArrayList<>(0)).build();
    }
}

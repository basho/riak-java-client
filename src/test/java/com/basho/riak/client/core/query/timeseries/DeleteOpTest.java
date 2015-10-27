package com.basho.riak.client.core.query.timeseries;

import com.basho.riak.client.core.operations.ts.DeleteOperation;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by alex on 10/27/15.
 */
public class DeleteOpTest
{
    private final long now = 1443796900000l; // "now"
    private final String tableName = "my_table";
    private final BinaryValue tableNameBV = BinaryValue.createFromUtf8(tableName);
    private final List<Cell> keyValues = Arrays.asList(new Cell("my_family"),
                                                             new Cell("my_series"),
                                                             Cell.newTimestamp(now),
                                                             null);

    @Test
    public void shouldBuildADescriptiveQueryInfoString()
    {
        String expectedInfo = "DELETE { Cell{ my_family }, Cell{ my_series }, Cell{ 1443796900000 }, NULL } FROM TABLE my_table";
        DeleteOperation cmd = new DeleteOperation.Builder(tableNameBV, keyValues).build();
        assertEquals(expectedInfo, cmd.getQueryInfo().toStringUtf8());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionIfTableNameIsMissing()
    {
        DeleteOperation cmd = new DeleteOperation.Builder(null, keyValues).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionIfTableNameIsBlank()
    {
        DeleteOperation cmd = new DeleteOperation.Builder(BinaryValue.create(""), keyValues).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionIfKeysAreNull()
    {
        DeleteOperation cmd = new DeleteOperation.Builder(tableNameBV, null).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionIfKeysAreMissing()
    {
        DeleteOperation cmd = new DeleteOperation.Builder(tableNameBV, new ArrayList<Cell>(0)).build();
    }

}

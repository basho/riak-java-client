package com.basho.riak.client.core.query.timeseries;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by alex on 1/8/16.
 */
public class TableDefinitionTest
{
    @Test
    public void TestKeyColumnDescriptionsAreFiltered()
    {
        final TableDefinition foo = new TableDefinition("Foo", GetIdealTable());

        assertKeyCollectionsAreCorrect(foo);
    }

    @Test
    public void TestKeyColumnDescriptionsAreProperlyOrdered()
    {
        final TableDefinition foo = new TableDefinition("Foo", GetReverseTable());

        assertKeyCollectionsAreCorrect(foo);

        final TableDefinition bar = new TableDefinition("Bar", GetJumbledTable());

        assertKeyCollectionsAreCorrect(bar);

    }

    private void assertKeyCollectionsAreCorrect(TableDefinition foo)
    {
        assertFullColumnDefinitionsMatch(GetProperKeyList(), new ArrayList<>(foo.getLocalKeyColumnDescriptions()));
        assertFullColumnDefinitionsMatch(GetProperKeyList(), new ArrayList<>(foo.getPartitionKeyColumnDescriptions()));
    }

    private static List<FullColumnDescription> GetIdealTable()
    {
        return Arrays.asList(
            new FullColumnDescription("geohash", ColumnDescription.ColumnType.VARCHAR, false, 1),
            new FullColumnDescription("user", ColumnDescription.ColumnType.VARCHAR, false, 2),
            new FullColumnDescription("time", ColumnDescription.ColumnType.TIMESTAMP, false, 3),
            new FullColumnDescription("weather", ColumnDescription.ColumnType.VARCHAR, false),
            new FullColumnDescription("temperature", ColumnDescription.ColumnType.DOUBLE, true),
            new FullColumnDescription("uv_index", ColumnDescription.ColumnType.SINT64, true),
            new FullColumnDescription("observed", ColumnDescription.ColumnType.BOOLEAN, false));
    }

    private static List<FullColumnDescription> GetReverseTable()
    {
        final List<FullColumnDescription> cols = GetIdealTable();
        Collections.reverse(cols);
        return cols;
    }

    private static List<FullColumnDescription> GetJumbledTable()
    {
        return Arrays.asList(
                new FullColumnDescription("uv_index", ColumnDescription.ColumnType.SINT64, true),
                new FullColumnDescription("geohash", ColumnDescription.ColumnType.VARCHAR, false, 1),
                new FullColumnDescription("temperature", ColumnDescription.ColumnType.DOUBLE, true),
                new FullColumnDescription("time", ColumnDescription.ColumnType.TIMESTAMP, false, 3),
                new FullColumnDescription("user", ColumnDescription.ColumnType.VARCHAR, false, 2),
                new FullColumnDescription("observed", ColumnDescription.ColumnType.BOOLEAN, false),
                new FullColumnDescription("weather", ColumnDescription.ColumnType.VARCHAR, false));
    }

    private List<FullColumnDescription> GetProperKeyList()
    {
        return Arrays.asList(
                new FullColumnDescription("geohash", ColumnDescription.ColumnType.VARCHAR, false, 1),
                new FullColumnDescription("user", ColumnDescription.ColumnType.VARCHAR, false, 2),
                new FullColumnDescription("time", ColumnDescription.ColumnType.TIMESTAMP, false, 3));
    }

    public static void assertFullColumnDefinitionsMatch(List<FullColumnDescription> expectedSet,
                                                  List<FullColumnDescription> actualSet)
    {
        assertEquals(expectedSet.size(), actualSet.size());

        for (int i = 0; i < expectedSet.size(); i++)
        {
            final FullColumnDescription expected = expectedSet.get(i);
            final FullColumnDescription actual = actualSet.get(i);

            assertEquals(expected.getName(), actual.getName());
            assertEquals(expected.getType(), actual.getType());
            assertEquals(expected.isNullable(), actual.isNullable());
            assertEquals(expected.getLocalKeyOrdinal(), actual.getLocalKeyOrdinal());
            assertEquals(expected.getPartitionKeyOrdinal(), actual.getPartitionKeyOrdinal());
        }
    }
}

package com.basho.riak.client.core.query.timeseries;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static junit.framework.Assert.*;

/**
 * Created by alex on 10/14/15.
 */
public class ValidationTest
{
    @Test
    public void ValidateGoodRows()
    {
        TableDefinition def =
                new TableDefinition("Foo",
                                    new ColumnDescription("column1", ColumnDescription.ColumnType.BINARY, false),
                                    new ColumnDescription("column2", ColumnDescription.ColumnType.INTEGER, true),
                                    new ColumnDescription("column3", ColumnDescription.ColumnType.TIMESTAMP, true));

        List<Row> rows = Arrays.asList(
                new Row(new Cell("Foo"), new Cell(3), Cell.newTimestamp(12345l)),
                new Row(new Cell("Bar"), new Cell(42), Cell.newTimestamp(45678l)));

        TimeSeriesValidator.ValidationResult validationResult = TimeSeriesValidator.validateAll(def, rows);

        assertTrue(validationResult.isSuccess());
        assertNull(validationResult.getErrorMessage());
    }

    @Test
    public void ValidateBadRows()
    {
        TableDefinition def =
                new TableDefinition("Foo",
                                    new ColumnDescription("column1", ColumnDescription.ColumnType.BINARY, false),
                                    new ColumnDescription("column2", ColumnDescription.ColumnType.INTEGER, true),
                                    new ColumnDescription("column3", ColumnDescription.ColumnType.TIMESTAMP, true));

        List<Row> rows = Arrays.asList(
                new Row(new Cell("Foo"), Cell.newTimestamp(12345l), new Cell(false)),
                new Row(new Cell("Bar"), new Cell(42), Cell.newTimestamp(45678l)));

        TimeSeriesValidator.ValidationResult validationResult = TimeSeriesValidator.validateAll(def, rows);

        assertFalse(validationResult.isSuccess());
        assertNotNull(validationResult.getErrorMessage());
    }
}

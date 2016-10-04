/*
 * Copyright 2013-2016 Basho Technologies Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basho.riak.client.core.query.timeseries;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Parses DESCRIBE query results into {@link FullColumnDescription}s.
 * <p>
 * Expected Format for the DESCRIBE function is 5 or 7 columns depending on the version.
 * <p>
 * V1 includes:
 * "Column"        (non-null Varchar)
 * "Type"          (non-null Varchar)
 * "Is Null"       (non-null Boolean)
 * "Partition Key" (nullable SInt64)
 * "Local Key"     (nullable SInt64)
 * <p>
 * V2 also includes:
 * "Interval", part of the quantum information (nullable SInt64)
 * "Unit", part of the quantum information     (nullable Varchar), either 'd', 'h', 'm', or 's'
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.7
 */

class DescribeQueryResultParser
{
    private final static int NAME_IDX = 0;
    private final static int TYPE_IDX = 1;
    private final static int NULLABLE_IDX = 2;
    private final static int PARTITION_KEY_IDX = 3;
    private final static int LOCAL_KEY_IDX = 4;
    private final static int QUANTUM_INTERVAL_IDX = 5;
    private final static int QUANTUM_UNIT_IDX = 6;

    static List<FullColumnDescription> ConvertToColumnDescriptions(QueryResult queryResult)
    {
        final List<FullColumnDescription> fullColumnDescriptions = new ArrayList<>(queryResult.getRowsCount());

        for (Row row : queryResult)
        {
            fullColumnDescriptions.add(convertDescribeResultRowToFullColumnDescription(row));
        }

        return fullColumnDescriptions;
    }

    private static FullColumnDescription convertDescribeResultRowToFullColumnDescription(Row row)
    {
        final List<Cell> cells = row.getCellsCopy();

        assert (DescribeFnRowResultIsValid(cells));

        final String name = cells.get(NAME_IDX).getVarcharAsUTF8String();
        final String typeString = cells.get(TYPE_IDX).getVarcharAsUTF8String();
        final boolean isNullable = cells.get(NULLABLE_IDX).getBoolean();

        final Integer partitionKeyOrdinal = parseKeyCell(cells.get(PARTITION_KEY_IDX));
        final Integer localKeyOrdinal = parseKeyCell(cells.get(LOCAL_KEY_IDX));

        final ColumnDescription.ColumnType type =
                ColumnDescription.ColumnType.valueOf(typeString.toUpperCase(Locale.ENGLISH));

        final Quantum quantum = parseQuantumCells(cells);

        return new FullColumnDescription(name, type, isNullable, partitionKeyOrdinal, localKeyOrdinal, quantum);
    }

    private static Integer parseKeyCell(Cell keyCell)
    {
        final boolean isKeyMember = keyCell != null;
        return isKeyMember ? new Long(keyCell.getLong()).intValue() : null;
    }

    private static Quantum parseQuantumCells(List<Cell> cells)
    {
        if (cells.size() < 7)
        {
            return null;
        }

        final Cell quantumIntervalCell = cells.get(QUANTUM_INTERVAL_IDX);
        final Cell quantumUnitCell = cells.get(QUANTUM_UNIT_IDX);

        final boolean hasQuantum = quantumIntervalCell != null && quantumUnitCell != null;

        if (!hasQuantum)
        {
            return null;
        }

        final Long quantumInterval = quantumIntervalCell.getLong();
        final TimeUnit quantumUnit = Quantum.parseTimeUnit(quantumUnitCell.getVarcharAsUTF8String());

        return new Quantum(quantumInterval.intValue(), quantumUnit);
    }

    private static boolean DescribeFnRowResultIsValid(List<Cell> cells)
    {
        final boolean describeBaseIsValid = DescribeRowV1ChunkIsValid(cells);
        final boolean isValidV1Description = describeBaseIsValid && cells.size() == 5;
        final boolean isValidV2Description =
                describeBaseIsValid && cells.size() == 7 && DescribeRowV2ChunkIsValid(cells);

        return isValidV1Description || isValidV2Description;
    }

    private static boolean DescribeRowV1ChunkIsValid(List<Cell> cells)
    {
        if (cells.size() < 5)
        {
            return false;
        }

        final Cell partitionKeyCell = cells.get(PARTITION_KEY_IDX);
        final Cell localKeyCell = cells.get(LOCAL_KEY_IDX);

        return  cells.get(NAME_IDX).hasVarcharValue() &&
                cells.get(TYPE_IDX).hasVarcharValue() &&
                cells.get(NULLABLE_IDX).hasBoolean() &&
                partitionKeyCell != null ? partitionKeyCell.hasLong() : true &&
                localKeyCell != null ? localKeyCell.hasLong() : true;
    }

    private static boolean DescribeRowV2ChunkIsValid(List<Cell> cells)
    {
        if (cells.size() < 7)
        {
            return false;
        }

        final Cell quantumIntervalCell = cells.get(QUANTUM_INTERVAL_IDX);
        final Cell quantumUnitCell = cells.get(QUANTUM_UNIT_IDX);

        return quantumIntervalCell != null ? quantumIntervalCell.hasLong() : true &&
                quantumUnitCell != null ? quantumUnitCell.hasVarcharValue() : true;
    }
}

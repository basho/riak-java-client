package com.basho.riak.client.core.query.timeseries;

import java.util.List;

/**
 * Validates a row or set of rows against a known Table Schema.
 */
public class TimeSeriesValidator
{
    public static ValidationResult validate(TableDefinition definition, Row row)
    {
        ColumnDescription.ColumnType[] schemaPattern = getSchemaPattern(definition);

        final String errorMessage = validateRow(definition, schemaPattern, row);

        if(errorMessage != null)
        {
            return new ValidationResult(false, String.format("Error Validating Row: %s", errorMessage));
        }

        return new ValidationResult(true);
    }

    public static ValidationResult validateAll(TableDefinition definition, List<Row> rows)
    {
        ColumnDescription.ColumnType[] schemaPattern = getSchemaPattern(definition);

        for (int i = 0; i < rows.size(); i++)
        {
            final String errorMessage = validateRow(definition, schemaPattern, rows.get(i));

            if(errorMessage != null)
            {
                return new ValidationResult(false, String.format("Error Validating Row %d, %s", i+1, errorMessage));
            }
        }

        return new ValidationResult(true);
    }

    private static String validateRow(TableDefinition definition,
                                      ColumnDescription.ColumnType[] schemaPattern,
                                      Row row)
    {
        short i = 0;
        String errorMessage = null;

        for (Cell cell : row.getCells())
        {
            ColumnDescription.ColumnType foundType = getColumnTypeForCell(cell);
            if(foundType != schemaPattern[i])
            {
                final String columnName = definition.getColumnDescriptions().get(i).getName();
                final String expectedTypeString = schemaPattern[i].toString();
                String foundTypeString;

                if(foundType != null)
                {
                    foundTypeString = foundType.toString();
                }
                else
                {
                    foundTypeString = "NULL";
                }

                errorMessage = String.format("Column %d (\"%s\"). Was expecting <%s>, found <%s>.",
                                             i+1, columnName, expectedTypeString, foundTypeString);
                break;
            }

            i++;
        }

        return errorMessage;
    }

    private static ColumnDescription.ColumnType getColumnTypeForCell(Cell cell)
    {
        ColumnDescription.ColumnType foundType;
        if(cell.hasBinaryValue())
        {
            foundType = ColumnDescription.ColumnType.BINARY;
        }
        else if(cell.hasBoolean())
        {
            foundType = ColumnDescription.ColumnType.BOOLEAN;
        }
        else if(cell.hasLong())
        {
            foundType = ColumnDescription.ColumnType.INTEGER;
        }
        else if(cell.hasNumeric())
        {
            foundType = ColumnDescription.ColumnType.NUMERIC;
        }
        else if(cell.hasTimestamp())
        {
            foundType = ColumnDescription.ColumnType.TIMESTAMP;
        }
        else if(cell.hasSet())
        {
            foundType = ColumnDescription.ColumnType.SET;
        }
        else if(cell.hasMap())
        {
            foundType = ColumnDescription.ColumnType.MAP;
        }
        else // No type found... error.
        {
            foundType = null;
        }

        return foundType;
    }

    private static ColumnDescription.ColumnType[] getSchemaPattern(TableDefinition tableDefinition)
    {
        final int numColumns = tableDefinition.getColumnDescriptions().size();
        final ColumnDescription.ColumnType[] expectedOrder = new ColumnDescription.ColumnType[numColumns];

        for (int i = 0; i < numColumns; i++)
        {
            expectedOrder[i] = tableDefinition.getColumnDescriptions().get(i).getType();
        }

        return expectedOrder;
    }

    public static class ValidationResult
    {
        private final boolean success;
        private final String errorMessage;

        public ValidationResult(boolean success)
        {
            this(success, null);
        }

        public ValidationResult(boolean success, String errorMessage)
        {
            this.success = success;
            this.errorMessage = errorMessage;
        }

        public boolean isSuccess()
        {
            return success;
        }

        public String getErrorMessage()
        {
            return errorMessage;
        }
    }
}

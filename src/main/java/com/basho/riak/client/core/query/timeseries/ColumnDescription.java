package com.basho.riak.client.core.query.timeseries;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Describes a column in a Riak Time Series table.
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public class ColumnDescription
{
    private final String name;
    private final ColumnType type;
    private final Collection<ColumnType> complexType;
    private final boolean nullable;

    public ColumnDescription(String name, ColumnType type)
    {
        this(name, type, null, false);
    }

    public ColumnDescription(String name, ColumnType type, boolean nullable)
    {
       this(name, type, null, nullable);
    }

    ColumnDescription(String name,
                             ColumnType type,
                             Collection<ColumnType> complexType,
                             boolean nullable)
    {
        this.name = name;
        this.type = type;
        this.complexType = complexType;
        this.nullable = nullable;
    }

    /**
     * Get the name of the column.
     * @return The name of the column.
     */
    public String getName()
    {
        return name;
    }

    /**
     * Get the type of the column.
     * @return The type of the column.
     */
    public ColumnType getType()
    {
        return type;
    }

//    /**
//     * Get the complex type of the column.
//     * @return The complex type of the column.
//     */
//    public Collection<ColumnType> getComplexType()
//    {
//        return complexType;
//    }

    /**
     * Get whether the column is nullable.
     * @return Whether these columns can be null or not.
     */
    public boolean isNullable()
    {
        return nullable;
    }

    /**
     * A collection of Column Types.
     */
    public enum ColumnType
    {
        BINARY(0),
        INTEGER(1),
        NUMERIC(2),
        TIMESTAMP(3),
        BOOLEAN(4),
        SET(5),
        MAP(6);

        private static Map<Integer, ColumnType> map = new HashMap<Integer, ColumnType>();

        static {
            for (ColumnType cTypeEnum : ColumnType.values()) {
                map.put(cTypeEnum.id, cTypeEnum);
            }
        }

        private final int id;

        ColumnType(int id)
        {
            this.id = id;
        }

        public static ColumnType valueOf(int columnType)
        {
            return map.get(columnType);
        }

        public int getId()
        {
            return this.id;
        }
    }
}

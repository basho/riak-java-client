package com.basho.riak.client.core.query.timeseries;

import java.util.HashMap;
import java.util.Map;

/**
 * A Metadata description of a column in Riak Time Series.
 * Contains a column name and column type.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public class ColumnDescription
{
    private final String name;
    private final ColumnType type;

    public ColumnDescription(String name, ColumnType type)
    {
        this.name = name;
        this.type = type;
    }

    public String getName()
    {
        return name;
    }

    public ColumnType getType()
    {
        return type;
    }


    public enum ColumnType
    {
        BINARY(0),
        SINT64(1),
        DOUBLE(2),
        TIMESTAMP(3),
        BOOLEAN(4);

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

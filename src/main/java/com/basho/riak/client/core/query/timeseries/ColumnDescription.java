package com.basho.riak.client.core.query.timeseries;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public class ColumnDescription
{
    private String name;
    private ColumnType type;
    private Collection<ColumnType> complexType;

    public ColumnDescription(String name, ColumnType type, Collection<ColumnType> complexType)
    {
        this.name = name;
        this.type = type;
        this.complexType = complexType;
    }

    public String getName()
    {
        return name;
    }

    public ColumnType getType()
    {
        return type;
    }

    public Collection<ColumnType> getComplexType()
    {
        return complexType;
    }

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

        private int id;

        private ColumnType(int id)
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

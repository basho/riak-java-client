package com.basho.riak.client.core.query.timeseries;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

/**
 * Created by alex on 9/2/15.
 */
public class MapCellTest
{
    @Test
    public void TestPojoSet()
    {
        ComplexPojo pojo1 = new ComplexPojo();
        pojo1.value = "foo";
        pojo1.iVal = 42;
        pojo1.fVal = 2.5f;

        MapCell mapCell = MapCell.fromObject(pojo1, new TypeReference<ComplexPojo>() {});

        assertTrue(mapCell.hasMap());

        ComplexPojo pojo2 = MapCell.getObject(mapCell, new TypeReference<ComplexPojo>() {});

        assertEquals(pojo1, pojo2);
    }
}

class ComplexPojo
{
    public String value;
    public int iVal;
    public float fVal;

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        ComplexPojo that = (ComplexPojo) o;

        if (iVal != that.iVal)
        {
            return false;
        }
        if (Float.compare(that.fVal, fVal) != 0)
        {
            return false;
        }
        return !(value != null ? !value.equals(that.value) : that.value != null);

    }

    @Override
    public int hashCode()
    {
        int result = value != null ? value.hashCode() : 0;
        result = 31 * result + iVal;
        result = 31 * result + (fVal != +0.0f ? Float.floatToIntBits(fVal) : 0);
        return result;
    }
}

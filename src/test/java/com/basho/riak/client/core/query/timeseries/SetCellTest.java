package com.basho.riak.client.core.query.timeseries;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static junit.framework.Assert.assertTrue;

public class SetCellTest
{
    @Test
    public void TestStringSet()
    {
        HashSet<String> set = new HashSet<String>(Arrays.asList("foo", "bar", "baz"));
        SetCell setCell = SetCell.fromSet(set, new TypeReference<String>() {});

        assertTrue(setCell.hasSet());

        Set<String> set1 = SetCell.getSet(setCell, new TypeReference<String>() {});

        assertTrue(set1.contains("foo"));
        assertTrue(set1.contains("bar"));
        assertTrue(set1.contains("baz"));
    }

    @Test
    public void TestPojoSet()
    {
        Pojo p1 = new Pojo();
        p1.value = "foo";
        Pojo p2 = new Pojo();
        p2.value = "bar";
        Pojo p3 = new Pojo();
        p3.value = "baz";
        HashSet<Pojo> set = new HashSet<Pojo>(Arrays.asList(p1, p2, p3));

        SetCell setCell = SetCell.fromSet(set, new TypeReference<Pojo>() {});

        assertTrue(setCell.hasSet());

        Set<Pojo> set1 = SetCell.getSet(setCell, new TypeReference<Pojo>() {});

        assertTrue(set1.contains(p1));
        assertTrue(set1.contains(p2));
        assertTrue(set1.contains(p3));
    }
}

class Pojo
{
    public String value;

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

        Pojo pojo = (Pojo) o;

        return !(value != null ? !value.equals(pojo.value) : pojo.value != null);

    }

    @Override
    public int hashCode()
    {
        return value != null ? value.hashCode() : 0;
    }
}

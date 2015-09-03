package com.basho.riak.client.core.query.timeseries;

import com.basho.riak.client.api.convert.Converter;
import com.basho.riak.client.api.convert.ConverterFactory;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.client.core.util.Constants;
import com.fasterxml.jackson.core.type.TypeReference;

import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Set;

public class SetCell<T> extends Cell
{
    private static final Type StringType = new TypeReference<String>() {}.getType();
    private static final Namespace GenericNamespace = new Namespace("T");
    private static final BinaryValue GenericValue = BinaryValue.create("");

    public static <T> SetCell<T> fromSet(Set<T> value, TypeReference<T> typeReference)
    {
        SetCell<T> cell = new SetCell<T>();
        Converter<T> converter = ConverterFactory.getInstance().getConverter(typeReference);

        cell.setValue = new byte[value.size()][];
        int i = 0;

        for (T t : value)
        {
            Converter.OrmExtracted encodedValue = converter.fromDomain(t, GenericNamespace, GenericValue);
            cell.setValue[i] = encodedValue.getRiakObject().getValue().getValue();
            i++;
        }

        return cell;
    }

    public static <T> Set<T> getSet(Cell cell, TypeReference<T> typeReference)
    {
        Converter<T> converter = ConverterFactory.getInstance().getConverter(typeReference);
        HashSet<T> set = new HashSet<T>(cell.getSet().length);

        for (byte[] entry : cell.getSet())
        {
            if(typeReference.getType() == StringType)
            {
                set.add((T) BinaryValue.create(entry).toString());
            }
            else
            {
                T t = converter.toDomain(BinaryValue.create(entry), Constants.CTYPE_JSON);
                set.add(t);
            }
        }

        return set;
    }
}

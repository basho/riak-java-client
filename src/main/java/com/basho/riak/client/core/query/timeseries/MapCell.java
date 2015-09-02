package com.basho.riak.client.core.query.timeseries;

import com.basho.riak.client.api.convert.Converter;
import com.basho.riak.client.api.convert.ConverterFactory;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.client.core.util.Constants;
import com.fasterxml.jackson.core.type.TypeReference;


public class MapCell<T> extends Cell
{
    public static <T> MapCell<T> fromObject(T value, TypeReference<T> typeReference)
    {
        MapCell<T> cell = new MapCell<T>();
        Converter<T> converter = ConverterFactory.getInstance().getConverter(typeReference);

        Converter.OrmExtracted encodedValue = converter.fromDomain(value, new Namespace("T"), BinaryValue.create(""));
        cell.mapValue = encodedValue.getRiakObject().getValue().getValue();

        return cell;
    }

    public static <T> T getObject(Cell cell, TypeReference<T> typeReference)
    {
        Converter<T> converter = ConverterFactory.getInstance().getConverter(typeReference);
        T obj;

        if(typeReference.getType() == new TypeReference<String>(){}.getType())
        {
            // If we've gotten this far, then the user is using the map to store a string...
            obj = (T) BinaryValue.create(cell.getMap()).toString();
        }
        else
        {
            obj = converter.toDomain(BinaryValue.create(cell.getMap()), Constants.CTYPE_JSON);
        }

        return obj;
    }
}

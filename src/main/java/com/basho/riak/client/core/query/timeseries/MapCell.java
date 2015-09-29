package com.basho.riak.client.core.query.timeseries;

import com.basho.riak.client.api.convert.Converter;
import com.basho.riak.client.api.convert.ConverterFactory;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.client.core.util.Constants;
import com.fasterxml.jackson.core.type.TypeReference;

import java.lang.reflect.Type;


public class MapCell extends Cell
{
    private static final Type StringType = new TypeReference<String>() {}.getType();
    private static final Namespace GenericNamespace = new Namespace("T");
    private static final BinaryValue GenericValue = BinaryValue.create("");

    public static <T> MapCell fromObject(T value, TypeReference<T> typeReference)
    {
        MapCell cell = new MapCell();
        Converter<T> converter = ConverterFactory.getInstance().getConverter(typeReference);

        Converter.OrmExtracted encodedValue = converter.fromDomain(value, GenericNamespace, GenericValue);
        cell.mapValue = encodedValue.getRiakObject().getValue().getValue();

        return cell;
    }

    @SuppressWarnings("unchecked")
    public static <T> T getObject(Cell cell, TypeReference<T> typeReference)
    {
        T obj;
        Converter<T> converter = ConverterFactory.getInstance().getConverter(typeReference);

        if(typeReference.getType() == StringType)
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

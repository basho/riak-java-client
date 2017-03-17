package com.basho.riak.client;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.cap.VClock;
import com.basho.riak.client.api.commands.indexes.BinIndexQuery;
import com.basho.riak.client.api.commands.indexes.IntIndexQuery;
import com.basho.riak.client.api.commands.indexes.SecondaryIndexQuery;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.indexes.LongIntIndex;
import com.basho.riak.client.core.query.indexes.RiakIndex;
import com.basho.riak.client.core.query.indexes.RiakIndexes;
import com.basho.riak.client.core.query.indexes.StringBinIndex;
import com.basho.riak.client.core.util.BinaryValue;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import net.javacrumbs.jsonunit.core.internal.JsonUtils;
import net.javacrumbs.jsonunit.core.internal.NodeFactory;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class RiakTestFunctions
{
    public static class RiakObjectData
    {
        public String key;
        public Object value;
        public Map<String, Object> indices;
    }

    protected static Logger logger = LoggerFactory.getLogger(RiakTestFunctions.class);

    /**
     * Tolerant mapper that doesn't require quotation for field names
     * and allows to use single quote for string values
     */
    protected final static ObjectMapper tolerantMapper = initializeJsonUnitMapper();

    /**
     *  Making JsonAssert to be more tolerant to JSON format.
     *  And add some useful serializers
     */
    private static ObjectMapper initializeJsonUnitMapper()
    {
        final Object converter;
        try
        {
            converter = FieldUtils.readStaticField(JsonUtils.class, "converter", true);

            @SuppressWarnings("unchecked")
            final List<NodeFactory> factories = (List<NodeFactory>) FieldUtils.readField(converter, "factories", true);

            ObjectMapper mapper;
            for (NodeFactory nf: factories)
            {
                if (nf.getClass().getSimpleName().equals("Jackson2NodeFactory"))
                {
                    mapper = (ObjectMapper) FieldUtils.readField(nf, "mapper", true);

                    mapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true)
                            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                            .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
                            .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
                            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                            .registerModule( new SimpleModule()
                                    .addSerializer(VClock.class, new VClockSerializer())
                            );

                    return mapper;
                }
            }
        }
        catch (IllegalAccessException e)
        {
            throw new IllegalStateException("Can't initialize Jackson2 ObjectMapper because of UE", e);
        }

        throw new IllegalStateException("Can't initialize Jackson2 ObjectMapper, Jackson2NodeFactory is not found");
    }

    protected static List<Map.Entry<String, RiakObject>> parseRiakObjectsFromJsonData(String json) throws IOException
    {
        assert json != null && !json.isEmpty();

        String actualJson = json;

        // Add a list semantic if needed
        if (!json.trim().startsWith("["))
        {
            actualJson = "[\n" + json + "\n]";
        }

        final List<RiakObjectData> data = tolerantMapper.readValue(actualJson, new TypeReference<List<RiakTestFunctions.RiakObjectData>>(){});
        final List<Map.Entry<String, RiakObject>> r = new ArrayList<>(data.size());

        for (RiakObjectData rod: data)
        {
            final RiakObject ro = new RiakObject();
            final Map.Entry<String, RiakObject> e = new AbstractMap.SimpleEntry<>(rod.key, ro);

            r.add(e);

            // populate value, if any
            if( rod.value != null)
            {
                if ( rod.value instanceof Map || rod instanceof Collection)
                {
                    final String v = tolerantMapper.writerWithDefaultPrettyPrinter()
                            .writeValueAsString(rod.value);

                    ro.setContentType("application/json")
                            .setValue(BinaryValue.create(v));
                }
                else
                {
                    ro.setContentType("text/plain")
                        .setValue(BinaryValue.create(rod.value.toString()));
                }
            }

            // populate 2i, if any
            if (rod.indices == null || rod.indices.isEmpty())
            {
                continue;
            }

            final RiakIndexes riakIndexes = ro.getIndexes();
            for (Map.Entry<String, Object> ie: rod.indices.entrySet())
            {
                assert ie.getValue() != null;

                if (ie.getValue() instanceof Long)
                {
                    riakIndexes.getIndex(LongIntIndex.named(ie.getKey()))
                        .add((Long)ie.getValue());
                }
                else if (ie.getValue() instanceof Integer)
                {
                    riakIndexes.getIndex(LongIntIndex.named(ie.getKey()))
                            .add(((Integer)ie.getValue()).longValue());
                }
                else if (ie.getValue() instanceof String)
                {
                    riakIndexes.getIndex(StringBinIndex.named(ie.getKey()))
                            .add((String)ie.getValue());
                }
                else throw new IllegalStateException("Unsupported 2i value type '" +
                            ie.getValue().getClass().getName() + "'");
            }
        }

        return r;
    }

    public static void createKVData(RiakClient client, Namespace ns, String jsonData) throws IOException, ExecutionException, InterruptedException
    {
        final List<Map.Entry<String, RiakObject>> parsedData = parseRiakObjectsFromJsonData(jsonData);

        for (Map.Entry<String, RiakObject> pd: parsedData)
        {
            final String key = createKValue(client, ns, pd.getKey(), pd.getValue(), true);
        }
    }

    protected static String createKValue(RiakClient client, Location location,
                                         Object value, Boolean checkCreation ) throws ExecutionException, InterruptedException
    {
        return createKValue(client, location.getNamespace(), location.getKeyAsString(), value, checkCreation);
    }

    protected static String createKValue(RiakClient client, Namespace ns, String key,
                                         Object value, Boolean checkCreation ) throws ExecutionException, InterruptedException
    {
        final StoreValue.Builder builder = new StoreValue.Builder(value)
                .withOption(StoreValue.Option.PW, Quorum.allQuorum());

        // Use provided key, if any
        if (key != null && !key.isEmpty())
        {
            builder.withLocation(new Location(ns, key));
        }
        else
        {
            builder.withNamespace(ns);
        }

        final StoreValue cmd = builder
                .withOption(StoreValue.Option.W, new Quorum(1))
                .build();

        final StoreValue.Response r = client.execute(cmd);

        final String realKey = r.hasGeneratedKey() ? r.getGeneratedKey().toStringUtf8() : key;

        if (checkCreation)
        {
            // -- check creation to be 100% sure that everything was created properly
            final Location location = new Location(ns, BinaryValue.create(realKey));

            FetchValue.Response fetchResponse = null;

            for (int retryCount=6; retryCount>=0; --retryCount)
            {
                try
                {
                    fetchResponse = fetchByLocation(client, location);
                }
                catch (IllegalStateException ex)
                {
                    if (ex.getMessage().startsWith("Nothing was found") && retryCount > 1)
                    {
                        logger.trace("Value for '{}' hasn't been created yet, attempt {}", location, retryCount+1);
                        Thread.sleep(200);
                        continue;
                    }

                    throw ex;
                }
            }


            // As soon as value is reachable by a key, it is expected that it also will be reachable by 2i

            final RiakObject etalonRObj = value instanceof RiakObject ?
                    (RiakObject) value : fetchResponse.getValue(RiakObject.class);

            for (RiakIndex<?> ri : etalonRObj.getIndexes())
            {
                assert(ri.values().size() == 1);

                ri.values().forEach( v-> {
                    try {
                        final List<Location> locations = query2i(client, ns, ri.getName(), v);

                        throwIllegalStateIf( !locations.contains(location),
                                "Location '%s' is not reachable by 2i '%s'",
                                location, ri.getName());

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }

        return realKey;
    }

    protected static void throwIllegalStateIf(Boolean flag, String format, Object... args) throws IllegalStateException
    {
        if (flag)
        {
            throw new IllegalStateException(String.format(format, args));
        }
    }

    protected static <T> List<Location> query2i(RiakClient client, Namespace ns,
            String indexName, T value) throws ExecutionException, InterruptedException
    {
        SecondaryIndexQuery<?,?, ?> cmd = null;

        if (value instanceof String)
        {
            cmd = new BinIndexQuery.Builder(ns, indexName, (String)value).build();
        }
        else if (value instanceof Integer)
        {
            cmd = new IntIndexQuery.Builder(ns, indexName, ((Integer)value).longValue()).build();
        }
        else if (value instanceof Long)
        {
            cmd = new IntIndexQuery.Builder(ns, indexName, (Long)value).build();
        }
        else  throwIllegalStateIf(true, "Type '%s' is not suitable for 2i", value.getClass().getName());

        return client.execute(cmd)
                .getEntries().stream()
                    .map(e->e.getRiakObjectLocation())
                    .collect(Collectors.toList());
    }

    protected static <V> V fetchByLocationAs(RiakClient client, Location location, Class<V> valueClazz)
            throws ExecutionException, InterruptedException
    {
        final FetchValue.Response r = fetchByLocation(client, location);

        throwIllegalStateIf(r.isNotFound(), "Nothing was found for location '%s'", location);
        throwIllegalStateIf(r.getNumberOfValues() > 1,
                "Fetch by Location '$location' returns more than one result: %d were actually returned",
                r.getNumberOfValues());

        final V v = r.getValue(valueClazz);
        return v;
    }

    protected static FetchValue.Response fetchByLocation(RiakClient client, Location location)
            throws ExecutionException, InterruptedException
    {
        final FetchValue cmd = new FetchValue.Builder(location).build();
        final FetchValue.Response r = client.execute(cmd);

        return r;
    }

    private static class VClockSerializer extends JsonSerializer<VClock>
    {
        @Override
        public void serialize(VClock value, JsonGenerator gen, SerializerProvider serializers) throws IOException, JsonProcessingException {
            // Due to lack of support binary values in JsonUnit it is required to perform manual conversion to Base64
            //gen.writeBinary(value.getBytes());
            gen.writeString(Base64.getEncoder().encodeToString(value.getBytes()));
        }
    }
}
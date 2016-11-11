package com.basho.riak.client.api.commands.search;

import com.basho.riak.client.api.AsIsRiakCommand;
import com.basho.riak.client.core.operations.YzPutSchemaOperation;
import com.basho.riak.client.core.query.search.YokozunaSchema;

/**
 * Command used to store a search schema in Riak.
 * <p>
 * To store a schema for Solr/Yokozuna in Riak, you must supply a
 * {@link com.basho.riak.client.core.query.search.YokozunaSchema} that defines the schema.
 * </p>
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public final class StoreSchema extends AsIsRiakCommand<Void, YokozunaSchema>
{
    private final YokozunaSchema schema;

    StoreSchema(Builder builder)
    {
        this.schema = builder.schema;
    }

    @Override
    protected YzPutSchemaOperation buildCoreOperation()
    {
        return new YzPutSchemaOperation.Builder(schema).build();
    }

    /**
     * Builder for a StoreSchema command.
     */
    public static class Builder
    {
        private final YokozunaSchema schema;

        /**
         * Construct a Builder for a StoreSchema command.
         *
         * @param schema The schema to be stored to Riak.
         */
        public Builder(YokozunaSchema schema)
        {
            this.schema = schema;
        }

        /**
         * Construct the StoreSchema command.
         * @return the new StoreSchema command.
         */
        public StoreSchema build()
        {
            return new StoreSchema(this);
        }
    }
}

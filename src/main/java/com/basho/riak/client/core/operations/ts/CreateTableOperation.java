/*
 * Copyright 2013-2016 Basho Technologies Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basho.riak.client.core.operations.ts;

import com.basho.riak.client.core.operations.PBFutureOperation;
import com.basho.riak.client.core.query.timeseries.ColumnDescription;
import com.basho.riak.client.core.query.timeseries.FullColumnDescription;
import com.basho.riak.client.core.query.timeseries.TableDefinition;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakTsPB;
import com.google.protobuf.ByteString;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * An operation to create a Riak Time Series table according to the provided definition.
 *
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.6
 */
public class CreateTableOperation extends PBFutureOperation<Void, RiakTsPB.TsQueryResp, String>
{
    private final RiakTsPB.TsQueryReq.Builder reqBuilder;
    private final String queryText;

    private CreateTableOperation(AbstractBuilder builder)
    {
        super(RiakMessageCodes.MSG_TsQueryReq,
                RiakMessageCodes.MSG_TsQueryResp,
                builder.reqBuilder,
                RiakTsPB.TsQueryResp.PARSER);

        this.reqBuilder = builder.reqBuilder;
        this.queryText = builder.queryText;
    }

    @Override
    protected Void convert(List<RiakTsPB.TsQueryResp> rawResponse)
    {
        return null;
    }

    @Override
    public String getQueryInfo()
    {
        return this.queryText;
    }

    public static abstract class AbstractBuilder<R, THIS extends AbstractBuilder>
    {
        private RiakTsPB.TsQueryReq.Builder reqBuilder;
        private String queryText;
        private int quantum;
        private char quantumUnit;
        private final TableDefinition tableDefinition;

        public AbstractBuilder(TableDefinition tableDefinition)
        {
            if (tableDefinition == null)
            {
                throw new IllegalArgumentException("TableDefinition cannot be null.");
            }

            final String tableName = tableDefinition.getTableName();
            if (tableName == null || tableName.length() == 0)
            {
                throw new IllegalArgumentException("Table Name cannot be null or empty");
            }

            this.tableDefinition = tableDefinition;
        }

        public abstract R build();


        public CreateTableOperation buildOperation()
        {
            final String keys = generateKeys(tableDefinition, quantum, quantumUnit).toString();

            queryText = String.format("CREATE TABLE %s (%s,\n\n PRIMARY KEY (%s))",
                    tableDefinition.getTableName(), generateColumns(tableDefinition),
                    keys);

            reqBuilder = RiakTsPB.TsQueryReq.newBuilder()
                    .setQuery(RiakTsPB.TsInterpolation.newBuilder().setBase(
                            ByteString.copyFromUtf8(queryText)
                    ));


            return new CreateTableOperation(this);
        }


        @SuppressWarnings("unchecked")
        public THIS withQuantum(int quantum, TimeUnit tu)
        {
            switch (tu)
            {
                case SECONDS:
                    quantumUnit = 's';
                    break;

                case MINUTES:
                    quantumUnit = 'm';
                    break;

                case HOURS:
                    quantumUnit = 'h';
                    break;

                case DAYS:
                    quantumUnit = 'd';
                    break;

                    default:
                        throw new IllegalArgumentException("Unsupported quantum unit '"+ tu.name() +"', at the moment the only:" +
                                " seconds, minutes, hours and days are supported.");
            }

            this.quantum = quantum;
            return (THIS)this;
        }

        private static StringBuilder generateColumns(TableDefinition tableDefinition)
        {
            final StringBuilder sb = new StringBuilder();

            for (FullColumnDescription fd: tableDefinition.getFullColumnDescriptions())
            {
                if (sb.length() > 0)
                {
                    sb.append(",\n ");
                }

                sb.append(fd.getName())
                        .append(' ')
                        .append(fd.getType().name());

                if (!fd.isNullable())
                {
                    sb.append(" not null");
                }
            }

            return sb;
        }

        private static StringBuilder generateKeys(TableDefinition tableDefinition, int quantum, char quantumUnit)
        {

            final Collection<FullColumnDescription> pks = tableDefinition.getPartitionKeyColumnDescriptions();
            if (pks == null || pks.isEmpty())
            {
                throw new IllegalArgumentException("No defined primary keys, at least one primary key should be defined.");
            }

            boolean quantumGenerated = false;
            final StringBuilder sb = new StringBuilder();
            for (FullColumnDescription k: pks)
            {
                if (sb.length() > 0)
                {
                    sb.append(", ");
                }
                else
                {
                    sb.append('(');
                }

                if (!quantumGenerated && ColumnDescription.ColumnType.TIMESTAMP.equals(k.getType()))
                {
                    // handle quantum
                    sb.append("quantum(")
                        .append(k.getName())
                        .append(',')
                        .append(quantum)
                        .append(',')
                        .append(quantumUnit)
                        .append(')');

                    quantumGenerated = true;
                }
                else
                {
                    sb.append(k.getName());
                }
            }
            sb.append(')');

            for (FullColumnDescription lk: tableDefinition.getLocalKeyColumnDescriptions())
            {
                sb.append(", ")
                        .append(lk.getName());
            }

            return sb;
        }
    }
}

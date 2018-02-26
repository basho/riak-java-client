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
package com.basho.riak.client.api.commands;

import com.basho.riak.client.api.commands.timeseries.CreateTable;
import com.basho.riak.client.core.operations.ts.CreateTableOperation;
import com.basho.riak.client.core.query.timeseries.ColumnDescription;
import com.basho.riak.client.core.query.timeseries.FullColumnDescription;
import com.basho.riak.client.core.query.timeseries.Quantum;
import com.basho.riak.client.core.query.timeseries.TableDefinition;
import com.basho.riak.protobuf.RiakTsPB;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CreateTableTest extends OperationTestBase<CreateTableOperation>
{
    private static final TableDefinition tableDefinition = new TableDefinition("TestTable", Arrays.asList(
            new FullColumnDescription("field1", ColumnDescription.ColumnType.TIMESTAMP,  false, 1, 1),
            new FullColumnDescription("field2", ColumnDescription.ColumnType.SINT64, false, null, 2),
            new FullColumnDescription("field3", ColumnDescription.ColumnType.BOOLEAN, true),
            new FullColumnDescription("field4", ColumnDescription.ColumnType.DOUBLE, false),
            new FullColumnDescription("field5", ColumnDescription.ColumnType.VARCHAR, true)
    ));

    private static final TableDefinition newTableDefinition = new TableDefinition("TestTable", Arrays.asList(
            new FullColumnDescription("field1", ColumnDescription.ColumnType.TIMESTAMP,  false, 1, 1, new Quantum(1, TimeUnit.SECONDS)),
            new FullColumnDescription("field2", ColumnDescription.ColumnType.SINT64, false, null, 2),
            new FullColumnDescription("field3", ColumnDescription.ColumnType.BOOLEAN, true),
            new FullColumnDescription("field4", ColumnDescription.ColumnType.DOUBLE, false),
            new FullColumnDescription("field5", ColumnDescription.ColumnType.VARCHAR, true)
                                                                                                         ));

    private void verifyTableCreation(final TableDefinition td, Integer quantum,
                                     TimeUnit tm, String expected) throws ExecutionException, InterruptedException
    {
        final CreateTable.Builder cmdBuilder = new CreateTable.Builder(td);
        if (quantum != null && tm != null)
        {
            cmdBuilder.withQuantum(quantum, tm);
        }

        final CreateTable cmd = cmdBuilder.build();
        final CreateTableOperation operation = executeAndVerify(cmd);

        final RiakTsPB.TsQueryReq.Builder builder =
                (RiakTsPB.TsQueryReq.Builder) Whitebox.getInternalState(operation, "reqBuilder");

        final String query = builder.getQuery().getBase().toStringUtf8();
        assertEquals(normString(expected), normString(query));
    }

    @Test
    public void testCreateWithLocalKeyProvided() throws ExecutionException, InterruptedException
    {
        verifyTableCreation(
                tableDefinition,
                10,
                TimeUnit.SECONDS,
                "CREATE TABLE TestTable ("
                        + "field1 TIMESTAMP not null,\n"
                        + " field2 SINT64 not null,\n"
                        + " field3 BOOLEAN,\n"
                        + " field4 DOUBLE not null,\n"
                        + " field5 VARCHAR,\n\n"
                        + " primary key ((quantum(field1,10,s)), field1, field2))"
            );
    }

    @Test
    public void testCreateWithoutLocalKey() throws ExecutionException, InterruptedException
    {
        final TableDefinition td = new TableDefinition("TestTable2", Arrays.asList(
                new FullColumnDescription("geohash", ColumnDescription.ColumnType.VARCHAR, false, 1),
                new FullColumnDescription("user", ColumnDescription.ColumnType.VARCHAR, false, 2),
                new FullColumnDescription("time", ColumnDescription.ColumnType.TIMESTAMP, false, 3),
                new FullColumnDescription("weather", ColumnDescription.ColumnType.VARCHAR, false),
                new FullColumnDescription("temperature", ColumnDescription.ColumnType.DOUBLE, true),
                new FullColumnDescription("uv_index", ColumnDescription.ColumnType.SINT64, true),
                new FullColumnDescription("observed", ColumnDescription.ColumnType.BOOLEAN, false)
        ));

        verifyTableCreation(
                td,
                15,
                TimeUnit.MINUTES,
                "CREATE TABLE TestTable2 ("
                        + "geohash varchar not null,"
                        + " user varchar not null,"
                        + " time timestamp not null,"
                        + " weather varchar not null,"
                        + " temperature double,"
                        + " uv_index sint64,"
                        + " observed boolean not null,"
                        + " PRIMARY KEY ((geohash, user, quantum(time,15,m)), geohash, user, time))"
        );
    }

    @Test
    public void testNewFormatQuantumGetsUsed() throws ExecutionException, InterruptedException
    {
        verifyTableCreation(
                newTableDefinition, // Contains 1 second quantum
                null,
                null,
                "CREATE TABLE TestTable ("
                        + "field1 TIMESTAMP not null,\n"
                        + " field2 SINT64 not null,\n"
                        + " field3 BOOLEAN,\n"
                        + " field4 DOUBLE not null,\n"
                        + " field5 VARCHAR,\n\n"
                        + " primary key ((quantum(field1,1,s)), field1, field2))");
    }

    @Test
    public void testOldFormatQuantumOverridesNewFormat() throws ExecutionException, InterruptedException
    {
        verifyTableCreation(
                newTableDefinition, // Contains 1 second quantum
                15,
                TimeUnit.MINUTES,
                "CREATE TABLE TestTable ("
                        + "field1 TIMESTAMP not null,\n"
                        + " field2 SINT64 not null,\n"
                        + " field3 BOOLEAN,\n"
                        + " field4 DOUBLE not null,\n"
                        + " field5 VARCHAR,\n\n"
                        + " primary key ((quantum(field1,15,m)), field1, field2))");
    }

    @Test
    public void failOnUnsupportedTimeUnits() throws ExecutionException, InterruptedException
    {
        final EnumSet<TimeUnit> supported =  EnumSet.of(TimeUnit.DAYS, TimeUnit.HOURS,
                TimeUnit.MINUTES, TimeUnit.SECONDS);

        final EnumSet<TimeUnit> notSupported = EnumSet.complementOf(supported);

        for (TimeUnit tu: notSupported)
        {
            try
            {
                verifyTableCreation(tableDefinition, 10, tu, "");
            }
            catch (IllegalArgumentException ex)
            {
                assertTrue(ex.getMessage().startsWith("Unsupported quantum unit"));
                continue;
            }

            fail("In case of using unsupported time unit IllegalArgumentException must be thrown");
        }
    }

    private static String normString(final String str)
    {
        return str.replace("\n","")
            .replaceAll("\\s{2,}+"," ")
            .toLowerCase();
    }
}

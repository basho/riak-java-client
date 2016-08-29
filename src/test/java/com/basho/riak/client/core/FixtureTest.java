/*
 * Copyright 2013 BashoTechnologies Inc
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
package com.basho.riak.client.core;

import com.basho.riak.client.core.fixture.NetworkTestFixture;
import java.io.IOException;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class FixtureTest
{
    static NetworkTestFixture fixture;
    static int startingPort = 5000;

    @BeforeClass
    public static void installFixture() throws IOException
    {
        fixture = new NetworkTestFixture(startingPort);
        new Thread(fixture).start();
    }

    @AfterClass
    public static void teardownFixture() throws IOException
    {
        fixture.shutdown();
    }
}

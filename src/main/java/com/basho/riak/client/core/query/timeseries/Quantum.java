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
package com.basho.riak.client.core.query.timeseries;

import java.util.concurrent.TimeUnit;


/**
 * Holds the Quantum information for a Riak TimeSeries row.
 * Used in conjunction with {@link FullColumnDescription} when receiving data
 * from a {@link com.basho.riak.client.api.commands.timeseries.DescribeTable} command,
 * or when creating a table with the {@link com.basho.riak.client.api.commands.timeseries.CreateTable} command.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.7
 */
public class Quantum
{
    private final int interval;
    private final TimeUnit unit;

    public Quantum(int interval, TimeUnit unit)
    {
        this.interval = interval;
        this.unit = unit;

        if (unit != TimeUnit.DAYS && unit != TimeUnit.HOURS && unit != TimeUnit.MINUTES && unit != TimeUnit.SECONDS)
        {
            throw new IllegalArgumentException("Time Unit must be either DAYS, HOURS, MINUTES, or SECONDS.");
        }
    }

    public int getInterval()
    {
        return interval;
    }

    public TimeUnit getUnit()
    {
        return unit;
    }

    public char getUnitAsChar()
    {
        return getTimeUnitChar(this.unit);
    }

    static TimeUnit parseTimeUnit(String timeUnitString)
    {
        switch (timeUnitString)
        {
            case "d":
                return TimeUnit.DAYS;
            case "h":
                return TimeUnit.HOURS;
            case "m":
                return TimeUnit.MINUTES;
            case "s":
                return TimeUnit.SECONDS;
            default:
                return null;
        }
    }

    public static char getTimeUnitChar(TimeUnit timeUnit)
    {
        switch (timeUnit)
        {
            case SECONDS:
                return 's';

            case MINUTES:
                return'm';

            case HOURS:
                return'h';

            case DAYS:
                return'd';

            default:
                throw new IllegalArgumentException("Unsupported quantum unit '"+ timeUnit.name() +"', at the moment the only:" +
                                                           " seconds, minutes, hours and days are supported.");
        }
    }
}

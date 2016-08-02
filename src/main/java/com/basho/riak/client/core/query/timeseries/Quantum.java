package com.basho.riak.client.core.query.timeseries;

import java.util.concurrent.TimeUnit;


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

    static TimeUnit parseTimeUnit(char timeUnitChar)
    {
        return parseTimeUnit(timeUnitChar + "");
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

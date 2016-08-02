package com.basho.riak.client.core.query.timeseries;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import com.basho.riak.client.core.util.HostAndPort;

public class CoveragePlanResult implements Iterable<CoverageEntry>
{
    private HashMap<HostAndPort, List<CoverageEntry>> perHostCoverage = new HashMap<HostAndPort, List<CoverageEntry>>();

    protected CoveragePlanResult()
    {
    }

    protected CoveragePlanResult(CoveragePlanResult rhs)
    {
        this.perHostCoverage.putAll(rhs.perHostCoverage);
    }

    public Set<HostAndPort> hosts()
    {
        return perHostCoverage.keySet();
    }

    public List<CoverageEntry> hostEntries(HostAndPort host)
    {
        final List<CoverageEntry> lst = perHostCoverage.get(host);

        if (lst == null)
        {
            return Collections.emptyList();
        }

        return lst;
    }

    public List<CoverageEntry> hostEntries(String host, int port)
    {
        return hostEntries(HostAndPort.fromParts(host, port));
    }

    private static <T> Iterator<T> emptyIterator()
    {
        return Collections.emptyIterator();
    }

    @Override
    public Iterator<CoverageEntry> iterator()
    {
        final Iterator<List<CoverageEntry>> itor = perHostCoverage.values().iterator();

        return new Iterator<CoverageEntry>()
        {
            Iterator<CoverageEntry> subIterator = null;

            @Override
            public boolean hasNext()
            {
                if (subIterator == null || !subIterator.hasNext())
                {
                    if (itor.hasNext())
                    {
                        subIterator = itor.next().iterator();
                    }
                    else
                    {
                        subIterator = emptyIterator();
                        return false;
                    }
                }

                return subIterator.hasNext();
            }

            @Override
            public CoverageEntry next()
            {
                if (!hasNext())
                {
                    throw new NoSuchElementException();
                }

                assert subIterator != null;
                return subIterator.next();
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    public void addEntry(CoverageEntry coverageEntry)
    {
        final HostAndPort key = HostAndPort.fromParts(coverageEntry.getHost(), coverageEntry.getPort());
        List<CoverageEntry> lst = perHostCoverage.get(key);
        if (lst == null)
        {
            lst = new LinkedList<CoverageEntry>();
            perHostCoverage.put(key, lst);
        }
        lst.add(coverageEntry);
    }
}
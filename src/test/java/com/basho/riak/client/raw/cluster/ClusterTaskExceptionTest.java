package com.basho.riak.client.raw.cluster;

import org.junit.Test;

import com.basho.riak.client.raw.query.MapReduceTimeoutException;

public class ClusterTaskExceptionTest {

    @Test public void throwIfCause_Match() {
        final ClusterTaskException cte = new ClusterTaskException(new MapReduceTimeoutException());
        boolean threwCorrectException = false;
        try {
            cte.throwCauseIf(MapReduceTimeoutException.class);
        } catch (MapReduceTimeoutException ex) {
            threwCorrectException = true;
        }
        assert threwCorrectException;
    }

    @Test public void throwIfCause_NoMatch() {
        final ClusterTaskException cte = new ClusterTaskException(new MapReduceTimeoutException());
        cte.throwCauseIf(RuntimeException.class);
    }

    @Test public void throwIfCause_NullCause() {
        final ClusterTaskException cte = new ClusterTaskException(null);
        cte.throwCauseIf(RuntimeException.class);
    }

    @Test public void throwIfCause_NullException() throws Exception {
        final ClusterTaskException cte = new ClusterTaskException(new MapReduceTimeoutException());
        cte.throwCauseIf(null);
    }
}

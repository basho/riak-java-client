package com.basho.riak.client.raw.cluster;

/**
 * A {@link RuntimeException} that can wrap any checked exceptions, allowing them
 * to be thrown later with calls to {@link #throwCauseIf(Class)}.
 *
 * @see com.basho.riak.client.raw.ClusterClient
 */
public class ClusterTaskException extends RuntimeException {

    public ClusterTaskException(final Throwable throwable) {
        super(throwable);
    }

    /**
     * If this instance's cause is the same type as the passed in class
     * this method will throw the cause.  Otherwise it does nothing.
     *
     * @param ex the {@link Class} of the type to compare against
     * @param <E> the type we expect to be thrown
     * @throws E if the cause matches what we're looking for we throw it
     */
    public <E extends Exception> void throwCauseIf(Class<E> ex) throws E {
        final Throwable cause = getCause();
        if (cause != null && ex != null && ex.isAssignableFrom(cause.getClass())) {
            //noinspection unchecked
            throw (E) getCause();
        }
    }
}

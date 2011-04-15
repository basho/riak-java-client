package com.basho.riak.newapi.cap;

import java.util.Collection;

/**
 * A conflict resolver that doesn't resolve conflict. If it is presented with a
 * collection of siblings it throws.
 * 
 * @author russell
 * 
 */
public final class DefaultResolver<T> implements ConflictResolver<T> {
    public T resolve(Collection<T> siblings) throws UnresolvedConflictException {
        if (siblings.size() > 1) {
            throw new UnresolvedConflictException("Siblings found", siblings);
        } else if (siblings.size() == 1) {
            return siblings.iterator().next();
        } else {
            return null;
        }
    }
}
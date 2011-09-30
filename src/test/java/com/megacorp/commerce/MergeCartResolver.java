package com.megacorp.commerce;

import java.util.Collection;
import java.util.HashSet;

import com.basho.riak.client.cap.ConflictResolver;

/**
 * A simple example of a conflict resolver for the ShoppingCart domain type.
 * 
 * Merge the contents of any siblings, worse case is deletes get undone.
 * 
 * @author russell
 * 
 */
public final class MergeCartResolver implements ConflictResolver<ShoppingCart> {

    public ShoppingCart resolve(Collection<ShoppingCart> siblings) {
        String userId = null;
        final Collection<String> items = new HashSet<String>();

        for (ShoppingCart c : siblings) {
            userId = c.getUserId();
            for (String item : c) {
                items.add(item);
            }
        }

        final ShoppingCart resolved = new ShoppingCart(userId);
        return resolved.addItems(items);
    }
}
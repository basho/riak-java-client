/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.basho.riak.client.bucket;

import com.basho.riak.client.RiakException;
import com.basho.riak.client.builders.DomainBucketBuilder;
import com.basho.riak.client.cap.ConflictResolver;
import com.basho.riak.client.cap.Mutation;
import com.basho.riak.client.cap.MutationProducer;
import com.basho.riak.client.cap.Retrier;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.convert.KeyUtil;
import com.basho.riak.client.convert.RiakKey;
import com.basho.riak.client.operations.DeleteObject;
import com.basho.riak.client.operations.FetchObject;
import com.basho.riak.client.operations.MultiFetchObject;
import com.basho.riak.client.operations.StoreObject;
import com.basho.riak.client.query.MultiFetchFuture;
import com.basho.riak.client.raw.DeleteMeta;
import com.basho.riak.client.raw.FetchMeta;
import com.basho.riak.client.raw.StoreMeta;
import java.util.Arrays;
import java.util.List;

/**
 * A domain bucket is a wrapper around a {@link Bucket} that is strongly typed and uses
 * a preset {@link ConflictResolver}, {@link MutationProducer}, {@link Converter}, r, w, dw, rw, {@link Retrier},
 * returnBody etc
 * 
 * <p>
 * If you are working with one specific type of data only it can be simpler to
 * create a {@link DomainBucket} around your bucket. It reduces the amount of
 * code since the {@link Converter}, {@link Mutation}, {@link Retrier} and
 * {@link ConflictResolver} are likely to be the same for each operation.
 * </p>
 * <p>
 * Example: 
 * <code><pre>
 * final Bucket b = client.createBucket(bucketName).allowSiblings(true).nVal(3).execute();
 * 
 * final DomainBucket<ShoppingCart> carts = DomainBucket.builder(b, ShoppingCart.class)
 *          .withResolver(new MergeCartResolver())
 *          .returnBody(true)
 *          .retrier(DefaultRetrier.attempts(3))
 *          .w(1)
 *          .dw(1)
 *          .r(1)
 *          .rw(1)
 *          .build();
 * 
 *  final ShoppingCart cart = new ShoppingCart(userId);
 * 
 *  cart.addItem("coffee");
 *  cart.addItem("fixie");
 *  cart.addItem("moleskine");
 * 
 *  final ShoppingCart storedCart = carts.store(cart);
 *  ShoppinCart cart2 = carts.fetch("userX");
 *  cart.addItem("toaster");
 *  carts.store(cart2);
 *  //etc
 * </pre></code>
 * </p>
 * 
 * @author russell
 * @see RiakBucket
 * @see DomainBucketBuilder
 * 
 */
public class DomainBucket<T> {

    private final Bucket bucket;
    private final ConflictResolver<T> resolver;
    private final Converter<T> converter;
    private final MutationProducer<T> mutationProducer;
    private final StoreMeta storeMeta;
    private final FetchMeta fetchMeta;
    private final DeleteMeta deleteMeta;
    private final Class<T> clazz;
    private final Retrier retrier;

  
    /**
     * Create a new {@link DomainBucket} for <code>clazz</code> Class objects
     * wrapped around <code>bucket</code>
     * 
     * It is recommended to use the {@link DomainBucketBuilder} rather than this
     * constructor.
     * 
     * @param bucket
     *            The bucket to wrap.
     * @param resolver
     *            the {@link ConflictResolver}
     * @param converter
     *            the {@link Converter} to use
     * @param mutationProducer
     *            the {@link MutationProducer} to use
     * @param storeMeta
     *            the set of store parameters
     * @param fetchMeta
     *            the set of fetch parameters
     * @param deleteMeta
     *            the set of delete parameters
     * @param clazz
     *            the Class type of the DomainBucket
     * @param retrier
     *            the {@link Retrier} to use for each operation
     */
    public DomainBucket(Bucket bucket, ConflictResolver<T> resolver, Converter<T> converter,
            MutationProducer<T> mutationProducer, StoreMeta storeMeta, FetchMeta fetchMeta, DeleteMeta deleteMeta,
            Class<T> clazz, final Retrier retrier) {
        this.bucket = bucket;
        this.resolver = resolver;
        this.converter = converter;
        this.mutationProducer = mutationProducer;
        this.storeMeta = storeMeta;
        this.fetchMeta = fetchMeta;
        this.deleteMeta = deleteMeta;
        this.clazz = clazz;
        this.retrier = retrier;
    }

    /**
     * Store <code>o</code> in Riak.
     * <code>T</code> must have a field annotated with {@link RiakKey}.
     * 
     * <p>
     * This is equivalent to creating and executing a {@link StoreObject}
     * operation with the {@link Converter}, {@link ConflictResolver},
     * {@link Retrier}, and a {@link Mutation} (from calling
     * {@link MutationProducer#produce(Object)} on <code>o</code>), r, w, dw
     * etc. passed when the DomainBucket was constructed.
     * </p>
     * 
     * @param o
     *            instance of <code.T</code> to store.
     * @return stored instance of <code>T</code>
     * @throws RiakException
     */
    public T store(T o) throws RiakException {
        final Mutation<T> mutation = mutationProducer.produce(o);
        final StoreObject<T> so = bucket.store(o)
            .withConverter(converter)
            .withMutator(mutation)
            .withResolver(resolver)
            .withRetrier(retrier);

        if (fetchMeta.getR() != null) {
            so.r(fetchMeta.getR());
        }

        if (storeMeta.hasW()) {
            so.w(storeMeta.getW());
        }

        if (storeMeta.hasDw()) {
            so.dw(storeMeta.getDw());
        }

        if (fetchMeta.hasPr()) {
            so.pr(fetchMeta.getPr());
        }

        if (storeMeta.hasPw()) {
            so.pw(storeMeta.getPw());
        }

        if (fetchMeta.hasBasicQuorum()) {
            so.basicQuorum(fetchMeta.getBasicQuorum());
        }

        if (fetchMeta.hasNotFoundOk()) {
            so.notFoundOK(fetchMeta.getNotFoundOK());
        }

        if (storeMeta.hasIfNotModified()) {
            so.ifNotModified(storeMeta.isIfNotModified());
        }

        if (storeMeta.hasIfNoneMatch()) {
            so.ifNoneMatch(storeMeta.isIfNoneMatch());
        }

        if (fetchMeta.hasReturnDeletedVClock()) {
            so.returnDeletedVClock(fetchMeta.getReturnDeletedVClock());
        }

        if (storeMeta.hasReturnBody()) {
            so.returnBody(storeMeta.getReturnBody());
        }

        return so.execute();
    }

    /**
     * Fetch data stored at <code>key</code> in this bucket as an instance of
     * <code>T</code>.
     * 
     * <p>
     * This is equivalent to creating and executing a {@link FetchObject}
     * configured with the {@link Converter}, {@link ConflictResolver},
     * {@link Retrier} and r value specified in the constructor.
     * </p>
     * 
     * @param key
     * @return
     * @throws RiakException
     */
    public T fetch(String key) throws RiakException {
        final FetchObject<T> fo = bucket.fetch(key, clazz)
            .withConverter(converter)
            .withResolver(resolver)
            .withRetrier(retrier);

        if (fetchMeta.hasR()) {
            fo.r(fetchMeta.getR());
        }

        if (fetchMeta.hasPr()) {
            fo.pr(fetchMeta.getPr());
        }

        if (fetchMeta.hasBasicQuorum()) {
            fo.basicQuorum(fetchMeta.getBasicQuorum());
        }

        if (fetchMeta.hasNotFoundOk()) {
            fo.notFoundOK(fetchMeta.getNotFoundOK());
        }
        return fo.execute();
    }

    /**
     * Fetch data stored at the key extracted from <code>o</code>'s
     * {@link RiakKey} annotated field as an instance of
     * <code>T</code>.
     * 
     * <p>
     * This is equivalent to creating and executing a {@link FetchObject}
     * configured with the {@link Converter}, {@link ConflictResolver},
     * {@link Retrier} and r value specified in the constructor.
     * </p>
     * 
     * @param key
     * @return
     * @throws RiakException
     */
    public T fetch(T o) throws RiakException {
        final FetchObject<T> fo = bucket.fetch(o)
            .withConverter(converter)
            .withResolver(resolver)
            .withRetrier(retrier);

        if (fetchMeta.hasR()) {
            fo.r(fetchMeta.getR());
        }

        if (fetchMeta.hasPr()) {
            fo.pr(fetchMeta.getPr());
        }

        if (fetchMeta.hasBasicQuorum()) {
            fo.basicQuorum(fetchMeta.getBasicQuorum());
        }

        if (fetchMeta.hasNotFoundOk()) {
            fo.notFoundOK(fetchMeta.getNotFoundOK());
        }
        return fo.execute();
    }

    /**
     * Fetch data stored at <code>keys</code> in this bucket as a List of
     * {@link MultiFetchFuture} objects that will return instances of {@code T}
     * 
     * <p>
     * This is equivalent to creating and executing a {@link MultiFetchObject}
     * configured with the {@link Converter}, {@link ConflictResolver},
     * {@link Retrier} and r value specified in the constructor.
     * </p>
     * 
     * @param keys
     * @return A list of futures
     * @see MultiFetchObject
     */
    public List<MultiFetchFuture<T>> multiFetch(String[] keys)
    {
        final MultiFetchObject<T> fo = bucket.multiFetch(Arrays.asList(keys), clazz)
            .withConverter(converter)
            .withResolver(resolver)
            .withRetrier(retrier);
        
            if (fetchMeta.hasR()) {
            fo.r(fetchMeta.getR());
        }

        if (fetchMeta.hasPr()) {
            fo.pr(fetchMeta.getPr());
        }

        if (fetchMeta.hasBasicQuorum()) {
            fo.basicQuorum(fetchMeta.getBasicQuorum());
        }

        if (fetchMeta.hasNotFoundOk()) {
            fo.notFoundOK(fetchMeta.getNotFoundOK());
        }
        return fo.execute();
            
    }
    
    /**
     * Fetch data stored at the keys extracted from each <code>obj</code>'s
     * {@link RiakKey} annotated field as an instance of
     * <code>T</code>.
     * 
     * <p>
     * This is equivalent to creating and executing a {@link MultiFetchObject}
     * configured with the {@link Converter}, {@link ConflictResolver},
     * {@link Retrier} and r value specified in the constructor.
     * </p>
     * 
     * @param keys
     * @return a list of futures
     * @see MultiFetchObject
     */
    public List<MultiFetchFuture<T>> multiFetch(List<T> objs)
    {
        final MultiFetchObject<T> fo = bucket.multiFetch(objs)
            .withConverter(converter)
            .withResolver(resolver)
            .withRetrier(retrier);
        
        if (fetchMeta.hasR()) {
            fo.r(fetchMeta.getR());
        }

        if (fetchMeta.hasPr()) {
            fo.pr(fetchMeta.getPr());
        }

        if (fetchMeta.hasBasicQuorum()) {
            fo.basicQuorum(fetchMeta.getBasicQuorum());
        }

        if (fetchMeta.hasNotFoundOk()) {
            fo.notFoundOK(fetchMeta.getNotFoundOK());
        }
        return fo.execute();
        
    }
    
    /**
     * Delete the key/value stored at the key extracted from <code>o</code>'s
     * {@link RiakKey} annotated field.
     * 
     * <p>
     * This is equivalent to creating and executing a {@link DeleteObject}
     * configured with the {@link Retrier} and r value specified in the
     * constructor.
     * </p>
     * 
     * @param key
     * @return
     * @throws RiakException
     */
    public void delete(T o) throws RiakException {
        final String key = KeyUtil.getKey(o);
        delete(key);
    }

    /**
     * Delete the key/value stored at the <code>key</code>
     * 
     * <p>
     * This is equivalent to creating and executing a {@link DeleteObject}
     * configured with the {@link Retrier} and r value specified in the
     * constructor.
     * </p>
     * 
     * @param key
     * @return
     * @throws RiakException
     */
    public void delete(String key) throws RiakException {
        final DeleteObject delete = bucket.delete(key).withRetrier(retrier);

        if (deleteMeta.hasR()) {
            delete.r(deleteMeta.getR());
        }

        if (deleteMeta.hasPr()) {
            delete.pr(deleteMeta.getPr());
        }

        if (deleteMeta.hasW()) {
            delete.w(deleteMeta.getW());
        }

        if (deleteMeta.hasDw()) {
            delete.dw(deleteMeta.getDw());
        }

        if (deleteMeta.hasPw()) {
            delete.pw(deleteMeta.getPw());
        }
        delete.fetchBeforeDelete(deleteMeta.hasVclock());
        delete.execute();
    }

    /**
     * Factory method to create a new {@link DomainBucketBuilder} for the given {@link Bucket} and Class.
     * @param b
     *            the Bucket to wrap
     * @param clazz the type of object to store/fetch with the new {@link DomainBucket}
     * @return a DomainBucketBuilder for the wrapped bucket
     */
    public static <T> DomainBucketBuilder<T> builder(Bucket b, Class<T> clazz) {
        return new DomainBucketBuilder<T>(b, clazz);
    }
}

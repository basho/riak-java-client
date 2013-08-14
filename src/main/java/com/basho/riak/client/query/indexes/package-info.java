/**
 * <p> Secondary Indexing (2i) in Riak gives developers the ability, at write
 * time, to tag an object stored in Riak with one or more queryable values.
 * 
 * <h4>Introduction</h4>
 * Since the KV data in Riak is completely opaque to 2i, the user must tell 2i exactly
 * what attribute to index on and what its index value should be via key/value
 * metadata. This is different from Search, which parses the data and builds
 * indexes based on a schema. 
 * <p>
 * The classes in this package provide an API for managing secondary indexes. 
 * </p>
 * <p><b>Important note:</b> 2i currently requires Riak to be configured to 
 * use the eleveldb or memory backend. The default bitcask backend does not support
 * 2i.
 * </p> 
 * <h4>Overview</h4>
 * In Riak there are two types of secondary indexes; "integer" and "binary". 
 * The former represents an index with numeric values whereas the second is used
 * for textual (String) data. The server API distinguishes between the two via 
 * a suffix ({@code "_int"} and {@code "_bin"} respectively) added to the index's 
 * name. In the client this is encapsulated in the 
 * {@link com.basho.riak.client.query.indexes.IndexType} enum. When specifying 
 * an index name you do not have to append this suffix; it's done automatically.
 * <p> A {@link com.basho.riak.client.query.indexes.RiakIndex} is made up of the index name, it's type,
 * then one or more queryable index values. 
 * </p>
 * <p>
 * {@code RiakIndex} instances are created and managed via the {@link com.basho.riak.client.query.indexes.RiakIndexes}
 * container. The container is stored in a {@link com.basho.riak.client.query.RiakObject}.
 * </p>
 * <h4>Working with RiakIndexes</h4>
 * <p>Data in Riak, including secondary indexes, is stored as raw bytes. The conversion
 * to and from bytes is handled by the concrete {@code RiakIndex} implementations 
 * and all indexes are managed by the {@code RiakIndexes} container. 
 * <p>
 * Each concrete {@code RiakIndex} includes a hybrid builder class named {@code Name}. 
 * The methods of this class take an instance of that builder as an 
 * argument to allow for proper type inference and construction of {@code RiakIndex}
 * objects to expose. 
 * </p>
 * <p>The {@code RiakIndexes}' {@code getIndex()} method will either return a reference to 
 * the existing {@code RiakIndex} or atomically add and return a new one. The
 * returned reference is of the type provided by the {@code Name} and is the 
 * mutable index; changes are made directly to it.
 * </p>
 * <blockquote><pre>
 * RiakIndexes myIndexes = riakObject.getIndexes();
 * LongIntIndex myIndex = myIndexes.getIndex(new LongIntIndex.Name("number_on_hand"));
 * myIndex.removeAll();
 * myIndex.add(6L);
 * </pre></blockquote>
 * <p>Calls can be chained, allowing for easy addition or removal of values from 
 * an index.
 * </p>
 * <blockquote><pre>
 * riakObject.getIndexes()
 *           .getIndex(new StringBinIndex.Name("colors"))
 *           .remove("blue")
 *           .add("red");
 * </pre></blockquote>
 * <h6>Special note when using RawIndex</h6>
 * A {@code RiakIndex} is uniquely identified by its textual name and {@code IndexType} 
 * regardless of the concrete {@code RiakIndex} implementation being used to view
 * or mutate it. This container enforces this uniqueness by being the source of 
 * all {@code RiakIndex} instances and managing them in a thread-safe way with 
 * atomic operations. 
 * <p>
 * What this means is that any {@code RiakIndex} having the same name and {@code Indextype}
 * will refer to the same index. This is only important to note if you are mixing 
 * access to the indexes using {@link com.basho.riak.client.query.indexes.RawIndex}. 
 * The test case below demonstrates
 * the relation.</p>
 * <blockquote><pre>
 * public void wrapping()
 * {
 *     // creates or fetches the BIN (_bin) index named "foo", adds a value to it  
 *     RawIndex index = indexes.getIndex(new RawIndex.Name("foo", IndexType.BIN));
 *     ByteArrayWrapper baw = ByteArrayWrapper.unsafeCreate("value".getBytes());
 *     index.add(baw);
 *       
 *     // fetches the previously created index as a StringBinIndex
 *     StringBinIndex wrapper = indexes.getIndex(new StringBinIndex.Name("foo"));
 *
 *     // The references are to different objects
 *     assertNotSame(index, wrapper);
 *     // The two objects are equal ( index.equals(wrapper) == true )
 *     assertEquals(index, wrapper);
 *     // The value exists
 *     assertTrue(wrapper.hasValue("value"));
 *     
 *     // Removing the value via the StringBinIndex is reflected in the RawIndex
 *     wrapper.remove("value");
 *     assertFalse(index.hasValue(baw));
 * }
 * </pre></blockquote>
 * <h6>Riak 2i _bin indexes and sorting</h6>
 * <p>
 * One of the key features of 2i is the ability to do range queries. As previously 
 * noted the values are stored in Riak as bytes. Comparison is done byte-by-byte. UTF-8
 * lends itself well to this as its byte ordering is the same as its lexical ordering.
 * </p> 
 * <p>
 * If you are using a {@code _bin} index with a character set whose byte ordering 
 * differs from its lexical ordering, range queries will be affected.
 * </p>
 * @see <a
 * href="http://docs.basho.com/riak/latest/dev/using/2i/">Using Secondary
 * Indexes in Riak</a>
 */
package com.basho.riak.client.query.indexes;
// TODO: add more info for using annotations once we get there
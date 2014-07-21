/**
 * Annotations to use for ORM.
 * 
 * <h4>Introduction</h4>
 * The Riak Java client provides a full set of annotations to facilitate simply
 * storing/retrieving your own domain object. All annotations can be applied to
 * a field or a getter/setter pair of methods. 
 * </p><p>
 * In addition, the {@link com.basho.riak.client.convert.Converter} 
 * interface allows for serialization/deserialization for the data portion. 
 * </p>
 * <p>
 * By annotating your own domain object, you can simply pass an instance of it
 * to {@link com.basho.riak.client.operations.kv.StoreValue}. 
 * </p>
 * <p>
 * When fetching data from Riak, the reverse is also true. The {@link com.basho.riak.client.operations.kv.FetchValue.Response}
 * handles injecting your domain object with any of the annotated values. 
 * <p>
 * <p>
 * Raw types as well as Generic types are supported. The latter is done via Jackson's
 * {@code TypeReferece} class. 
 * </p>
 * <h4>OverView</h4>
 * To store an object in Riak there's a minimum of four pieces of information
 * required; a bucket type, a bucket name, a key, and a vector clock. For an 
 * annotated domain object, only three of these are required as in the absence of 
 * a bucket type, the default "default" type is supplied. 
 * <pre>
 * <code>
 * public class AnnotatedPojo
 * {
 *     {@literal @}RiakBucketType
 *     public String bucketType;
 *     
 *     {@literal @}RiakBucketName
 *     public String bucketName;
 *     
 *     {@literal @}RiakKey
 *     public String key; 
 *     
 *     {@literal @}RiakVClock
 *     VClock clock;
 * 
 *     public String value;
 * }
 * </code>
 * </pre>
 * 
 */
package com.basho.riak.client.api.annotations;

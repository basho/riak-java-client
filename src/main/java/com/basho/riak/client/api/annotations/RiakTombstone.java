/*Licensed under the Apache License, Version 2.0 (the "License");
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
package com.basho.riak.client.api.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a field or getter/setter method pair in a class to serve as the tombstone indicator.
 * <p>
 * This annotation is used to denote a boolean field or getter/setter pair that will be marked true
 * if the object is a tombstone (deleted vector clock)
 * 
 * <pre>
 * public class MyPojo 
 * {
 *     {@literal @}RiakTombstone 
 *     public boolean tombstone;
 * }
 * 
 * public class AnotherPojo 
 * {
 *     private boolean tombstone;
 *     
 *     {@literal @}RiakTombstone 
 *     public boolean getTombstone() 
 *     {
 *         return tombstone;
 *     }
 * 
 *     {@literal @}RiakTombstone 
 *     public void setTombstone(boolean tombstone) 
 *     {
 *         this.tombstone = tombstone;
 *     }
 * }
 * </pre>
 * 
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */

@Retention(RetentionPolicy.RUNTIME) @Target({ElementType.FIELD, ElementType.METHOD}) public @interface RiakTombstone
{
}

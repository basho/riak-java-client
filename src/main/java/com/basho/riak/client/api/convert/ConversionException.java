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
package com.basho.riak.client.api.convert;

import com.basho.riak.client.core.query.RiakObject;

/**
 * Wraps any exception thrown while converting to/from {@link RiakObject} and your domain types.
 * @author russell
 *
 */
public class ConversionException extends RuntimeException
{
  /**
   * eclipse generated id
   */
  private static final long serialVersionUID = 2116948528090219193L;

  public ConversionException()
  {
    super();
  }

  public ConversionException(String message)
  {
    super(message);
  }

  public ConversionException(Throwable cause)
  {
    super(cause);
  }

  public ConversionException(String message, Throwable cause)
  {
    super(message, cause);
  }
}
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
package com.megacorp.commerce;

import org.codehaus.jackson.annotate.JsonProperty;

import com.basho.riak.client.convert.RiakKey;

public class GoogleStockDataItem {
    //{"Date":"2010-01-05","Open":627.18,"High":627.84,"Low":621.54,"Close":623.99,"Volume":3004700,"Adj. Close":623.99}
    @JsonProperty("Date")
    @RiakKey
    private String date;
    @JsonProperty("Open")
    private Double open;
    @JsonProperty("High")
    private Double high;
    @JsonProperty("Low")
    private Double low;
    @JsonProperty("Close")
    private Double close;
    @JsonProperty("Volume")
    private Long volume;
    @JsonProperty("Adj. Close")
    private Double adjustedClose;
    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((adjustedClose == null) ? 0 : adjustedClose.hashCode());
        result = prime * result + ((close == null) ? 0 : close.hashCode());
        result = prime * result + ((date == null) ? 0 : date.hashCode());
        result = prime * result + ((high == null) ? 0 : high.hashCode());
        result = prime * result + ((low == null) ? 0 : low.hashCode());
        result = prime * result + ((open == null) ? 0 : open.hashCode());
        result = prime * result + ((volume == null) ? 0 : volume.hashCode());
        return result;
    }
    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof GoogleStockDataItem)) {
            return false;
        }
        GoogleStockDataItem other = (GoogleStockDataItem) obj;
        if (adjustedClose == null) {
            if (other.adjustedClose != null) {
                return false;
            }
        } else if (!adjustedClose.equals(other.adjustedClose)) {
            return false;
        }
        if (close == null) {
            if (other.close != null) {
                return false;
            }
        } else if (!close.equals(other.close)) {
            return false;
        }
        if (date == null) {
            if (other.date != null) {
                return false;
            }
        } else if (!date.equals(other.date)) {
            return false;
        }
        if (high == null) {
            if (other.high != null) {
                return false;
            }
        } else if (!high.equals(other.high)) {
            return false;
        }
        if (low == null) {
            if (other.low != null) {
                return false;
            }
        } else if (!low.equals(other.low)) {
            return false;
        }
        if (open == null) {
            if (other.open != null) {
                return false;
            }
        } else if (!open.equals(other.open)) {
            return false;
        }
        if (volume == null) {
            if (other.volume != null) {
                return false;
            }
        } else if (!volume.equals(other.volume)) {
            return false;
        }
        return true;
    }
}
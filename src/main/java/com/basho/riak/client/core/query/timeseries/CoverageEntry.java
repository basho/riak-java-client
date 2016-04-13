package com.basho.riak.client.core.query.timeseries;

import java.io.Serializable;
import java.util.Arrays;

public class CoverageEntry implements Serializable {
    private static final long serialVersionUID = 0;
    private String host;
    private int port;
    private String fieldName;
    private long lowerBound;
    private boolean lowerBoundInclusive;
    private long upperBound;
    private boolean upperBoundInclusive;
    private String description;
    private byte[] coverageContext;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public long getLowerBound() {
        return lowerBound;
    }

    public void setLowerBound(long lowerBound) {
        this.lowerBound = lowerBound;
    }

    public boolean isLowerBoundInclusive() {
        return lowerBoundInclusive;
    }

    public void setLowerBoundInclusive(boolean lowerBoundInclusive) {
        this.lowerBoundInclusive = lowerBoundInclusive;
    }

    public long getUpperBound() {
        return upperBound;
    }

    public void setUpperBound(long upperBound) {
        this.upperBound = upperBound;
    }

    public boolean isUpperBoundInclusive() {
        return upperBoundInclusive;
    }

    public void setUpperBoundInclusive(boolean upperBoundInclusive) {
        this.upperBoundInclusive = upperBoundInclusive;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public byte[] getCoverageContext() {
        return coverageContext;
    }

    public void setCoverageContext(byte[] coverageContext) {
        this.coverageContext = coverageContext;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(coverageContext);
        result = prime * result + ((description == null) ? 0 : description.hashCode());
        result = prime * result + ((fieldName == null) ? 0 : fieldName.hashCode());
        result = prime * result + ((host == null) ? 0 : host.hashCode());
        result = prime * result + (int) (lowerBound ^ (lowerBound >>> 32));
        result = prime * result + (lowerBoundInclusive ? 1231 : 1237);
        result = prime * result + port;
        result = prime * result + (int) (upperBound ^ (upperBound >>> 32));
        result = prime * result + (upperBoundInclusive ? 1231 : 1237);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        CoverageEntry other = (CoverageEntry) obj;
        if (!Arrays.equals(coverageContext, other.coverageContext))
            return false;
        if (description == null) {
            if (other.description != null)
                return false;
        } else if (!description.equals(other.description))
            return false;
        if (fieldName == null) {
            if (other.fieldName != null)
                return false;
        } else if (!fieldName.equals(other.fieldName))
            return false;
        if (host == null) {
            if (other.host != null)
                return false;
        } else if (!host.equals(other.host))
            return false;
        if (lowerBound != other.lowerBound)
            return false;
        if (lowerBoundInclusive != other.lowerBoundInclusive)
            return false;
        if (port != other.port)
            return false;
        if (upperBound != other.upperBound)
            return false;
        if (upperBoundInclusive != other.upperBoundInclusive)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "CoverageEntry [host=" + host + ", port=" + port + ", fieldName=" + fieldName + ", lowerBound=" + lowerBound
                + ", lowerBoundInclusive=" + lowerBoundInclusive + ", upperBound=" + upperBound + ", upperBoundInclusive="
                + upperBoundInclusive + ", description=" + description + "]";
    }
}
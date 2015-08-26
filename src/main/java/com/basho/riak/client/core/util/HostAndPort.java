/*
 * Copyright 2013-2015 Basho Technologies Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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
package com.basho.riak.client.core.util;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * An immutable representation of a host and port.
 *
 * Note: Introduced  to reduce the amount of 3rd party dependencies such as guava
 *       (http://docs.guava-libraries.googlecode.com/git/javadoc/com/google/common/net/HostAndPort.html).
 *
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
*/
public final class HostAndPort implements Serializable {
    private static final long serialVersionUID = 0;
    private final int port;
    private final String host;
    private transient InetSocketAddress inetAddress;

    private HostAndPort(String host, int port){
        this.host = host;
        this.port = port;
    }

    public boolean hasPort() {
        return port >= 0;
    }

    public int getPort() {
        return port;
    }

    public int getPortOrDefault(int defaultPort){
        if(!hasPort()){
            return defaultPort;
        }
        return port;
    }

    public String getHost() {
        return host;
    }

    public InetSocketAddress asInetSocketAddress(){
        if(inetAddress == null){
            inetAddress = new InetSocketAddress(getHost(), getPort());
        }

        return inetAddress;
    }

    public static List<HostAndPort> hostsFromString(String hostPortStr, int defaultPort) {
        checkHost(hostPortStr);
        String rawHosts[] = hostPortStr.split(",");

        List<HostAndPort> retVal = new ArrayList<HostAndPort>(rawHosts.length);

        for( String s: rawHosts){
            retVal.add(HostAndPort.fromString(s, defaultPort));
        }

        return retVal;
    }

    public static HostAndPort fromString(String hostPortStr, int defaultPort) {
        hostPortStr = hostPortStr.trim();
        checkHost(hostPortStr);

        final int idx = hostPortStr.indexOf(':');
        final HostAndPort retVal;

        if( idx == -1 ){
            retVal = fromParts(hostPortStr, defaultPort);
        } else {
            try {
                retVal = fromParts(
                        hostPortStr.substring(0, idx),
                        Integer.parseInt(hostPortStr.substring(idx + 1))
                );
            }catch (NumberFormatException ex){
                throw new IllegalArgumentException("Unparseable port number: " + hostPortStr);
            }
        }
        return retVal;
    }

    public static HostAndPort fromParts(String host, int port) {
        checkHost(host);
        return new HostAndPort(host, port);
    }

    private static void checkHost(String host) throws IllegalArgumentException{
        if(host == null || host.isEmpty()){
            throw new IllegalArgumentException("Host must be provided, it can't be null or empty");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HostAndPort that = (HostAndPort) o;

        if (getPort() != that.getPort()) return false;
        return getHost().equals(that.getHost());
    }

    @Override
    public int hashCode() {
        int result = getPort();
        result = 31 * result + getHost().hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "HostAndPort{" +
                "host='" + host + '\'' +
                ", port=" + port +
                '}';
    }
}
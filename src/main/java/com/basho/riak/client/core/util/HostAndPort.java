package com.basho.riak.client.core.util;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by srg on 7/7/15.
 */
public class HostAndPort implements Serializable {
    private static final long serialVersionUID = 0;
    private final int port;
    private final String host;
    private transient InetSocketAddress inetAddress;

    private HostAndPort(String host, int port){
        this.host = host;
        this.port = port;
    }

    public int getPort() {
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
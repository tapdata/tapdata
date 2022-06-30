package io.tapdata.util;

import io.tapdata.kit.EmptyKit;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * @author samuel
 * @Description
 * @create 2022-04-26 12:04
 **/
public class NetUtil {

    private static final int DEFAULT_SOCKET_TIMEOUT_MS = 10 * 1000;

    /**
     * Check whether ip:port can be connected through socket
     *
     * @param host      IP ADDRESS
     * @param port      Port
     * @param timeoutMs TimeOut(millisecond)
     * @throws IOException              Represents a connection failure
     * @throws IllegalArgumentException The input parameter is incorrect
     */
    public static void validateHostPortWithSocket(String host, int port, int timeoutMs) throws IOException, IllegalArgumentException {
        if (EmptyKit.isEmpty(host)) throw new IllegalArgumentException("Host cannot be empty");
        if (port <= 0 || port >= 65536)
            throw new IllegalArgumentException("Port must greater than 0 and smaller then 65536");
        timeoutMs = timeoutMs <= 1000 ? DEFAULT_SOCKET_TIMEOUT_MS : timeoutMs;
        try {
            Socket s = new Socket();
            s.connect(new InetSocketAddress(host, port), timeoutMs);
            s.close();
        } catch (IOException e) {
            throw new IOException("Unable connect to " + host + ":" + port + ", reason: " + e.getMessage(), e);
        }
    }

    public static void validateHostPortWithSocket(String host, int port) throws IOException, IllegalArgumentException {
        validateHostPortWithSocket(host, port, DEFAULT_SOCKET_TIMEOUT_MS);
    }
}

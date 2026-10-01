package com.tapdata.http;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HttpUtilTest {

    @Test
    void openStreamReturnsHttpResponseBody() throws Exception {
        byte[] expected = "stream-content".getBytes(StandardCharsets.UTF_8);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/file", exchange -> {
            exchange.sendResponseHeaders(200, expected.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(expected);
            }
        });
        server.start();

        try {
            try (InputStream input = HttpUtil.openStream("http://127.0.0.1:" + server.getAddress().getPort() + "/file")) {
                assertArrayEquals(expected, input.readAllBytes());
            }
        } finally {
            server.stop(0);
        }
    }

    @Test
    void openStreamRejectsUnsuccessfulHttpStatus() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/missing", exchange -> {
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
        });
        server.start();

        try {
            assertThrows(HttpException.class,
                    () -> HttpUtil.openStream("http://127.0.0.1:" + server.getAddress().getPort() + "/missing"));
        } finally {
            server.stop(0);
        }
    }
}

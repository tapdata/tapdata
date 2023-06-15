package io.tapdata.wsclient.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class HttpUtils {
    public interface ErrorHandle {
        void error(int code, String message);
    }
    public static JSONObject post(String url, String data, Map<String, String> headers) throws IOException {
        return post(url, data, headers, null);
    }
    public static JSONObject post(String url, String data, Map<String, String> headers, ErrorHandle errorHandle) throws IOException {
        HttpURLConnection connection = getUrlConnection(url, "POST", headers);
        try {
//            output(connection, data);
            if(data != null) {
                connection.setDoOutput(true);
                connection.setRequestProperty( "Content-Type", "application/json");
                try(OutputStream outputStream = connection.getOutputStream()) {
                    outputStream.write(data.getBytes(StandardCharsets.UTF_8));
                }
            }
            connection.connect();
            int code = connection.getResponseCode();
            if(code >= 200 && code < 300) {
                return getJSONResult(url, connection);
            } else {
                if(errorHandle != null) {
                    errorHandle.error(code, connection.getResponseMessage());
                }
                throw new IOException("Url(post) " + url + " occur error, code " + code + " message " + connection.getResponseMessage());
            }
        } finally {
            connection.disconnect();
        }
    }
    public static JSONObject post(String url, JSONObject data, Map<String, String> headers) throws IOException {
        return post(url, JSON.toJSONString(data), headers);
    }

    private static void output(HttpURLConnection connection, JSONObject data) throws IOException {
        if(data != null) {
            connection.setDoOutput(true);
            connection.setRequestProperty( "Content-Type", "application/json");
            try(OutputStream outputStream = connection.getOutputStream()) {
                outputStream.write(JSON.toJSONString(data).getBytes("utf8"));
            }
        }
    }

    public static JSONObject get(String url, Map<String, String> headers) throws IOException {
        HttpURLConnection connection = getUrlConnection(url, "GET", headers);
        try {
            connection.connect();
            int code = connection.getResponseCode();
            if(code >= 200 && code < 300) {
                return getJSONResult(url, connection);
            } else {
                throw new IOException("Url(get) " + url + " occur error, code " + code + " message " + connection.getResponseMessage());
            }
        } finally {
            connection.disconnect();
        }
    }

    public static JSONObject delete(String url, Map<String, String> headers) throws IOException {
        HttpURLConnection connection = getUrlConnection(url, "DELETE", headers);
        try {
            connection.connect();
            int code = connection.getResponseCode();
            if(code >= 200 && code < 300) {
                return getJSONResult(url, connection);
            } else {
                throw new IOException("Url(delete) " + url + " occur error, code " + code + " message " + connection.getResponseMessage());
            }
        } finally {
            connection.disconnect();
        }
    }

    public static JSONObject put(String url, JSONObject data, Map<String, String> headers) throws IOException {
        HttpURLConnection connection = getUrlConnection(url, "PUT", headers);
        try {
            output(connection, data);
            connection.connect();
            int code = connection.getResponseCode();
            if(code >= 200 && code < 300) {
                return getJSONResult(url, connection);
            } else {
                throw new IOException("Url(put) " + url + " occur error, code " + code + " message " + connection.getResponseMessage());
            }
        } finally {
            connection.disconnect();
        }
    }

    private static JSONObject getJSONResult(String url, HttpURLConnection connection) throws IOException {
        try(InputStream inputStream = connection.getInputStream()) {
            String json = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            if(StringUtils.isNotBlank(json)) {
                JSONObject result = JSON.parseObject(json);
                if(result != null && result.getString("code").equals("ok")) {
                    JSONObject data = result.getJSONObject("data");
                    if(data != null) {
                        return data;
                    }
                }
            }
            throw new IOException("Url " + url + " content illegal, " + json);
        }
    }

    private static HttpURLConnection getUrlConnection(String url, String method, Map<String, String> headers) throws IOException {
        URL theUrl = new URL(url);
        HttpURLConnection connection = (HttpURLConnection) theUrl.openConnection();
        connection.setRequestMethod(method);
        if(headers != null) {
            for(Map.Entry<String, String> entry : headers.entrySet()) {
                connection.setRequestProperty(entry.getKey(), entry.getValue());
            }
        }
        return connection;
    }
}

package com.tapdata.tm.sdk.test;

import com.tapdata.tm.sdk.auth.BasicCredentials;
import com.tapdata.tm.sdk.auth.Signer;
import com.tapdata.tm.sdk.util.IOUtil;
import com.tapdata.tm.sdk.util.SignUtil;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/8/16 11:34
 */
public class TestLoadSchemaMain {
    public static void main(String[] args) throws IOException {

        String body = "/Users/lg/tmp/body.json";
        boolean enableCompression = true;
        if (args != null) {
            if (args.length > 0) {
                body = args[0];
            }
            if (args.length > 1) {
                enableCompression = "true".equalsIgnoreCase(args[1]);
            }
        }



        new TestLoadSchemaMain().testLoadSchema(body, enableCompression);
    }

    public void testLoadSchema(String filePath, boolean enableCompression) throws IOException {
        String uri = "https://cloud.tapdata.net/console/v3/tm/api/Connections/update";
        String ak = "hyQBbYy5yqXuCZFwVqGj5XEccrGWaRk1";
        String sk = "aBOxmIiQjYrLAg6NIeEu4vEuK75c6Sn4";

        Map<String, String> params = new HashMap<>();
        params.put("access_token", "c962db562c8241c98926021f0c2706417fa62c1a00194cb3b8b1eb1dfdb2aedf");
        params.put("where", "{ \"_id\" : \"6576a5f4b37b9461bfb18620\" }");
        params.put("ts", String.valueOf(System.currentTimeMillis()));
        params.put("nonce", UUID.randomUUID().toString());
        params.put("signVersion", "1.0");
        params.put("accessKey", ak);
        params.put("sign", "test");

        String body = IOUtil.readAsString(new FileInputStream(filePath));

        final String method = "POST";

        BasicCredentials basicCredentials = new BasicCredentials(ak, sk);
        Signer signer = Signer.getSigner(basicCredentials);

        String canonicalQueryString = SignUtil.canonicalQueryString(params);
        String stringToSign = String.format("%s:%s:%s", method, canonicalQueryString, body);
        String sign = signer.signString(stringToSign, basicCredentials);
        params.put("sign", sign);

        System.out.println("String to sign: " + stringToSign);
        System.out.println("Sign: " + sign);

        String queryString = params.keySet().stream().map(key -> {
            try {
                return String.format("%s=%s", SignUtil.percentEncode(key), SignUtil.percentEncode(params.get(key)));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            return key + "=" + params.get(key);
        }).collect(Collectors.joining("&"));

        HttpRequest request = new HttpRequest(uri + "?" + queryString, method);
        request.contentType("application/json");
        request.header("requestId", UUID.randomUUID().toString());

        byte[] data = body.getBytes(Charset.defaultCharset());
        if (enableCompression) {
            request.header("Content-Encoding", "gzip");

            ByteArrayOutputStream compressionOutput = new ByteArrayOutputStream();
            GZIPOutputStream gzipOutputStream = new GZIPOutputStream(compressionOutput);
            gzipOutputStream.write(data);
            gzipOutputStream.flush();
            gzipOutputStream.finish();
            data = compressionOutput.toByteArray();

            compressionOutput.close();
            gzipOutputStream.close();
        }

        logRequest(request);
        request.send(data);

        if (request.code() == 200) {
            System.out.println("Success");
        }

        logResponse(request);

        String response = request.body();
        System.out.println(response);
    }

    private void logRequest(HttpRequest request) {
        System.out.println(request.method() + " " + request.url().toString());
        System.out.println("Request headers:");
        request.getConnection().getRequestProperties().entrySet().forEach(entry -> {
            System.out.println("> " + entry.getKey() + ": " + entry.getValue());
        });
    }

    private void logResponse(HttpRequest request) {
        System.out.println(request.code());
        System.out.println("Response headers:");
        request.headers().entrySet().forEach(entry -> {
            System.out.println("< " + entry.getKey() + ": " + entry.getValue());
        });
    }
}

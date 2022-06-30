package com.tapdata.tm.sdk.test;

import com.tapdata.tm.sdk.auth.BasicCredentials;
import com.tapdata.tm.sdk.auth.Signer;
import com.tapdata.tm.sdk.util.SignUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/8/3 下午3:38
 * @description
 */
public class TestSigner {

    @Test
    public void testSign() {

        Map<String, String> params = new HashMap<>();
        params.put("signVersion", "1.0");
        params.put("ts", "111111");
        params.put("sign", "test");
        params.put("nonce", "xxxxxx");
        params.put("accessKey", "test");

        BasicCredentials basicCredentials = new BasicCredentials("test", "test");
        Signer signer = Signer.getSigner(basicCredentials);
        String canonicalQueryString = SignUtil.canonicalQueryString(params);

        String stringToSign = "POST:" + canonicalQueryString + ":";
        System.out.println(stringToSign);
        System.out.println(signer.signString(stringToSign, basicCredentials));
    }

    @Test
    public void testPercentEncode() throws UnsupportedEncodingException {
        String string = "A-Za-z0-9-_.!~*'()";
        String result = SignUtil.percentEncode(string);
        Assert.assertEquals(result, string);

        string = ";,/?:@&=+$";
        result = SignUtil.percentEncode(string);
        Assert.assertEquals(result, "%3B%2C%2F%3F%3A%40%26%3D%2B%24");

        string = "#";
        result = SignUtil.percentEncode(string);
        Assert.assertEquals(result, "%23");

        string = "ABC abc 123";
        result = SignUtil.percentEncode(string);
        Assert.assertEquals(result, "ABC%20abc%20123");
    }

    @Test
    public void testSignRequest() {

        final String accessKey = "4rPGQhEOChGhUOryAhgLiodaTuqsvXuv";
        final String secretKey = "4IqWVsLqshfc6c2Hk1bGsIzYmIpVgf6L";

        Map<String, String> params = new HashMap<>();
        params.put("ts", String.valueOf(System.currentTimeMillis()));
        params.put("nonce", UUID.randomUUID().toString());
        params.put("signVersion", "1.0");
        params.put("accessKey", "4rPGQhEOChGhUOryAhgLiodaTuqsvXuv");
        params.put("sign", "test");

        BasicCredentials basicCredentials = new BasicCredentials(accessKey, secretKey);
        Signer signer = Signer.getSigner(basicCredentials);

        final String method = "GET";
        String canonicalQueryString = SignUtil.canonicalQueryString(params);
        String stringToSign = String.format("%s:%s", method, canonicalQueryString);
        String sign = signer.signString(stringToSign, basicCredentials);
        params.put("sign", sign);

        String queryString = params.keySet().stream().map(key -> {
            try {
                return String.format("%s=%s", SignUtil.percentEncode(key), SignUtil.percentEncode(params.get(key)));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            return key + "=" + params.get(key);
        }).collect(Collectors.joining("&"));

        HttpRequest request = new HttpRequest("http://127.0.0.1:8086/tm/api/Settings?" + queryString, "GET");

        Assert.assertEquals(200, request.code());

        System.out.println(request.body());
    }

}

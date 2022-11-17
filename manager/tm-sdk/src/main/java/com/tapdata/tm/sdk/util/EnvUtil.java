package com.tapdata.tm.sdk.util;

import com.fasterxml.jackson.core.type.TypeReference;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.util.Map;
import java.util.Properties;

public class EnvUtil {

    private static final Properties properties = decodeToken(System.getenv("CLOUD_TOKEN"));

    public static Properties decodeToken(String token) {
        Properties prop = new Properties();
        if (token == null) {
            prop.setProperty("backend_url", System.getenv("backend_url"));
            prop.setProperty("process_id", System.getenv("process_id"));
            prop.setProperty("app_type", System.getenv("app_type"));
        } else {
            try {
                Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
                byte[] decode2 = java.util.Base64.getDecoder().decode(token);
                SecretKeySpec keyspec = new SecretKeySpec("5fa25b06ee34581d".getBytes(), "AES");
                IvParameterSpec ivspec = new IvParameterSpec("5fa25b06ee34581d".getBytes());
                cipher.init(2, keyspec, ivspec);
                byte[] result2 = cipher.doFinal(decode2);
                String resultStr2 = new String(result2);
                Map<String, Object> map = JacksonUtil.fromJson(new String(java.util.Base64.getDecoder().decode(resultStr2)), new TypeReference<Map<String, Object>>() {
                });
                prop.putAll(map);
                prop.setProperty("app_type", "dfs");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        prop.forEach((k, v) -> System.setProperty(k.toString(), v.toString()));
        return prop;
    }

    public static String get(String env) {
        return properties.getProperty(env);
    }
}

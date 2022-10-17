package com.tapdata.tm.utils;

import cn.hutool.core.codec.Base64;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

/**
 * 加密解密工具
 */
@Slf4j
public class EncrptAndDencryUtil {

    private static final String sKey = "dGcrwE7FobpZMjNKvkolwc9qxWagTQEPK8VPZxXmwC6DDDB5uGfQc3NQOYKFJSw0";

    // 加密
    public static String Encrypt(String sSrc) {
        String result = "";
        try {
            if (sKey == null) {
                log.error("Key为空null");
                return null;
            }
            // 判断Key是否为16位
            if (sKey.length() != 16) {
                log.error("Key长度不是16位");
                return null;
            }
            byte[] raw = sKey.getBytes("utf-8");
            SecretKeySpec skeySpec = new SecretKeySpec(raw, "AES");
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");//"算法/模式/补码方式"
            cipher.init(Cipher.ENCRYPT_MODE, skeySpec);
            byte[] encrypted = cipher.doFinal(sSrc.getBytes("utf-8"));

            result = new Base64().encode(encrypted);//此处使用BASE64做转码功能，同时能起到2次加密的作用。
        } catch (Exception e) {

        }
        return result;
    }

    // 解密
    public static String Decrypt(String sSrc) {
        String originalString = "";
        if (StringUtils.isEmpty(sSrc)) {
            return originalString;
        }


        try {
            // 判断Key是否正确
            if (sKey == null) {
                log.error("Key为空null");
                return null;
            }
            // 判断Key是否为16位
            if (sKey.length() != 16) {
                log.error("Key长度不是16位");
                return null;
            }
            byte[] raw = sKey.getBytes("utf-8");
            SecretKeySpec skeySpec = new SecretKeySpec(raw, "AES");
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            cipher.init(Cipher.DECRYPT_MODE, skeySpec);
            byte[] encrypted1 = new Base64().decode(sSrc);//先用base64解密
            try {
                byte[] original = cipher.doFinal(encrypted1);
                originalString = new String(original, "utf-8");

            } catch (Exception e) {
                log.error(e.toString());
                return null;
            }
        } catch (Exception ex) {
            log.error("加密异常", ex);
        }
        return originalString;
    }

}

package com.tapdata.tm.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * MD5 加密工具
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/5/10 19:34 Create
 */
public class MD5Utils {

    public static byte[] toBytes(byte[] bytes) {
        try {
            if (null != bytes && bytes.length > 0) {
                MessageDigest md = MessageDigest.getInstance("MD5");
                md.update(bytes);
                return md.digest();
            }
        } catch (NoSuchAlgorithmException ignore) {
        }
        return new byte[0];
    }

    public static byte[] toBytes(String s) {
        if (null != s) {
            return toBytes(s.getBytes());
        }
        return new byte[0];
    }

    public static String toHex(byte[] bytes, boolean upper) {
        byte[] hash = toBytes(bytes);
        if (hash.length == 0) {
            return "";
        }

        String format = upper ? "%02X" : "%02x";
        StringBuilder hexString = new StringBuilder();
        for (byte b : hash) {
            hexString.append(String.format(format, b));
        }
        return hexString.toString();
    }

    public static String toLowerHex(byte[] bytes) {
        return toHex(bytes, false);
    }

    public static String toUpperHex(byte[] bytes) {
        return toHex(bytes, true);
    }

    public static String toHex(String s, boolean upper) {
        if (null == s) {
            return "";
        }
        return toHex(s.getBytes(), upper);
    }

    public static String toLowerHex(String s) {
        return toHex(s, false);
    }

    public static String toUpperHex(String s) {
        return toHex(s, true);
    }
}

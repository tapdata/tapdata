package com.tapdata.constant;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Util {

    /**
     * Encodes a string 2 MD5
     *
     * @param str String to encode
     * @return Encoded String
     */
    public static String crypt(String str, boolean upper) {
        if (str == null || str.isEmpty()) {
            return "";
        }
        return crypt(str.getBytes(), upper);
    }

    public static String crypt(byte[] bytes, boolean upper) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            return "";
        }

        md.update(bytes);
        byte[] hash = md.digest();
        StringBuilder hexString = new StringBuilder();
        for (byte b : hash) {
            if ((0xff & b) < 0x10) {
                hexString.append("0").append(Integer.toHexString((0xFF & b)));
            } else {
                hexString.append(Integer.toHexString(0xFF & b));
            }
        }

        if (upper) {
            return hexString.toString().toUpperCase();
        } else {
            return hexString.toString();
        }
    }
}

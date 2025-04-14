package com.tapdata.constant;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/4/7 11:59 Create
 */
class MD5UtilTest {

    static final String COMPLEX_CHARS = "`1234567890-=[]\\;',./~!@#$%^&*()_+{}|:\"<>?中文\uD83D\uDE80";

    @Test
    void testCryptWithNullString() {
        String plainTxt = null;
        assertEquals("", MD5Util.crypt(plainTxt, false));
        assertEquals("", MD5Util.crypt(plainTxt, true));
    }

    @Test
    void testCryptWithEmptyString() {
        assertEquals("", MD5Util.crypt("", false));
        assertEquals("", MD5Util.crypt("", true));
    }

    @Test
    void testCryptWithSimpleStringLowercase() {
        assertEquals("5d41402abc4b2a76b9719d911017c592", MD5Util.crypt("hello", false));
    }

    @Test
    void testCryptWithSimpleStringUppercase() {
        assertEquals("5D41402ABC4B2A76B9719D911017C592", MD5Util.crypt("hello", true));
    }

    @Test
    void testCryptWithComplexStringLowercase() {
        assertEquals("8ea1ac319ef583ce6ef6d3d16766ea09", MD5Util.crypt(COMPLEX_CHARS, false));
    }

    @Test
    void testCryptWithComplexStringUppercase() {
        assertEquals("8EA1AC319EF583CE6EF6D3D16766EA09", MD5Util.crypt(COMPLEX_CHARS, true));
    }

    @Test
    void testCryptWithBytesLowercase() {
        byte[] bytes = "hello".getBytes();
        assertEquals("5d41402abc4b2a76b9719d911017c592", MD5Util.crypt(bytes, false));
    }

    @Test
    void testCryptWithBytesUppercase() {
        byte[] bytes = "hello".getBytes();
        assertEquals("5D41402ABC4B2A76B9719D911017C592", MD5Util.crypt(bytes, true));
    }
}

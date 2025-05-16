package com.tapdata.tm.utils;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mockStatic;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/5/12 14:30 Create
 */
public class MD5UtilsTest {
    static final String testStr = "`1234567890-=[]\\;',./~!@#$%^&*()_+{}|:\"<>?中文\uD83D\uDE80";
    static final byte[] testBytes = testStr.getBytes();
    static final String expectedUpperStr = "8EA1AC319EF583CE6EF6D3D16766EA09";
    static final String expectedLowerStr = expectedUpperStr.toLowerCase();

    @Nested
    class ToBytesMethodTest {

        @Test
        void testToBytes_WithNullInput() {
            byte[] result = MD5Utils.toBytes((byte[]) null);
            assertNotNull(result);
            assertEquals(0, result.length);
        }

        @Test
        void testToBytes_WithEmptyInput() {
            byte[] result = MD5Utils.toBytes(new byte[0]);
            assertNotNull(result);
            assertEquals(0, result.length);
        }

        @Test
        void testToBytes_WithEmptyInputString() {
            byte[] result = MD5Utils.toBytes("");
            assertNotNull(result);
            assertEquals(0, result.length);
        }

        @Test
        void testToBytes_WithNullInputString() {
            byte[] result = MD5Utils.toBytes((String) null);
            assertNotNull(result);
            assertEquals(0, result.length);
        }

        @Test
        void testToBytes_WithNoSuchAlgorithmException() {
            try (MockedStatic<MessageDigest> messageDigestMockedStatic = mockStatic(MessageDigest.class)) {
                messageDigestMockedStatic.when(() -> MessageDigest.getInstance("MD5")).thenThrow(new NoSuchAlgorithmException("test failed"));

                byte[] result = MD5Utils.toBytes(testBytes);
                assertNotNull(result);
                assertEquals(0, result.length);
            }
        }

    }

    @Nested
    class ToHexMethodTest {
        @Test
        void testToHex_WithValidBytesAndUpper() {
            String result = MD5Utils.toHex(testBytes, true);
            assertEquals(expectedUpperStr, result);
        }

        @Test
        void testToHex_WithValidBytesAndLower() {
            String result = MD5Utils.toHex(testBytes, false);
            assertEquals(expectedLowerStr, result);
        }

        @Test
        void testToLowerHex_WithValidBytes() {
            String result = MD5Utils.toLowerHex(testBytes);
            assertEquals(expectedLowerStr, result);
        }

        @Test
        void testToUpperHex_WithValidBytes() {
            String result = MD5Utils.toUpperHex(testBytes);
            assertEquals(expectedUpperStr, result);
        }
    }

    @Nested
    class StringOverloadMethodsTest {
        @Test
        void testToHex_WithStringInputAndUpper() {
            String result = MD5Utils.toHex(testStr, true);
            assertEquals(expectedUpperStr, result);
        }

        @Test
        void testToHex_WithStringInputAndLower() {
            String result = MD5Utils.toHex(testStr, false);
            assertEquals(expectedLowerStr, result);
        }

        @Test
        void testToLowerHex_WithStringInput() {
            String result = MD5Utils.toLowerHex(testStr);
            assertEquals(expectedLowerStr, result);
        }

        @Test
        void testToUpperHex_WithStringInput() {
            String result = MD5Utils.toUpperHex(testStr);
            assertEquals(expectedUpperStr, result);
        }

        @Test
        void testToHex_WithNullOrEmptyString() {
            String expected = "";

            String inputStr = null;
            assertEquals(expected, MD5Utils.toHex(inputStr, true));

            inputStr = "";
            assertEquals(expected, MD5Utils.toHex(inputStr, true));
        }
    }
}


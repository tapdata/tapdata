package io.tapdata.pdk.core.utils;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class IOUtils {
    public static final int EOF = -1;
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 4;
    public static String toString(InputStream inputStream) throws IOException {
        try(ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            copyLarge(inputStream, byteArrayOutputStream, new byte[DEFAULT_BUFFER_SIZE]);
            return new String(byteArrayOutputStream.toByteArray(), StandardCharsets.UTF_8);
        }
    }
    public static String toString(final byte[] input) {
        return new String(input, StandardCharsets.UTF_8);
    }

    public static String toString(final byte[] input, final String encoding) throws IOException {
        return new String(input, encoding);
    }
    public static long copyLarge(final InputStream input, final OutputStream output, final byte[] buffer)
            throws IOException {
        long count = 0;
        int n;
        while (EOF != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
            count += n;
        }
        return count;
    }
    public static long copyLarge(final Reader input, final Writer output) throws IOException {
        return copyLarge(input, output, new char[DEFAULT_BUFFER_SIZE]);
    }
    public static int copy(final Reader input, final Writer output) throws IOException {
        final long count = copyLarge(input, output);
        if (count > Integer.MAX_VALUE) {
            return -1;
        }
        return (int) count;
    }
    public static long copyLarge(final Reader input, final Writer output, final char[] buffer) throws IOException {
        long count = 0;
        int n;
        while (EOF != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
            count += n;
        }
        return count;
    }

}

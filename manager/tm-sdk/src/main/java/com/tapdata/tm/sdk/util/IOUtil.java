package com.tapdata.tm.sdk.util;

import java.io.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/10/21 下午3:56
 */
public class IOUtil {

    public static byte[] readFile(String file) throws IOException {
        return readFile(new File(file));
    }
    public static byte[] readFile(File file) throws IOException {
        return readInputStream(new FileInputStream(file));
    }

    public static byte[] readInputStream(InputStream inputStream) throws IOException {
        return readInputStream(inputStream, true);
    }
    public static byte[] readInputStream(InputStream inputStream, boolean closeInput) throws IOException {

        ByteArrayOutputStream output = new ByteArrayOutputStream();

        byte[] buf = new byte[1024];
        int length = inputStream.read(buf);
        while (length > 0) {
            output.write(buf, 0, length);
            length = inputStream.read(buf);
        }
        if (closeInput) {
            inputStream.close();
        }
        return output.toByteArray();
    }

    public static String readAsString(InputStream inputStream, boolean closeInput) throws IOException {
        byte[] data = readInputStream(inputStream, closeInput);
        return new String(data);
    }

    public static String readAsString(InputStream inputStream) throws IOException {
        return readAsString(inputStream, true);
    }

}

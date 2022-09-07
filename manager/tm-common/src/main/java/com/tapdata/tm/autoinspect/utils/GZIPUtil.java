package com.tapdata.tm.autoinspect.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/9/23 下午3:59
 * @description
 */
public class GZIPUtil {

    public static byte[] gzip(byte[] content) {
        if (content == null || content.length == 0) {
            return new byte[0];
        }
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutputStream;
        try {
            gzipOutputStream = new GZIPOutputStream(outputStream);
            gzipOutputStream.write(content);
            gzipOutputStream.flush();
            gzipOutputStream.close();
            return outputStream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static byte[] unGzip(byte[] content) {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(content);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try {
            GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream);
            byte[] buffer = new byte[1024];
            int length = 0;
            while ( (length = gzipInputStream.read(buffer)) >= 0 ) {
                outputStream.write(buffer, 0, length);
            }
            gzipInputStream.close();
            return outputStream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                outputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                inputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

}

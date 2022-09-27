package com.tapdata.tm.utils;

import lombok.extern.slf4j.Slf4j;

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
@Slf4j
public class GZIPUtil {

    public static byte[] gzip(byte[] content) {
        if (content == null || content.length == 0) {
            return new byte[0];
        }
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
             GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream)) {
            gzipOutputStream.write(content);
            gzipOutputStream.flush();
            gzipOutputStream.close();
            return outputStream.toByteArray();
        } catch (IOException e) {
            log.error("gzip error", e);
        }
        return null;
    }

    public static byte[] unGzip(byte[] content) {
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(content);
             ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
             GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream)) {
            byte[] buffer = new byte[1024];
            int length;
            while ( (length = gzipInputStream.read(buffer)) >= 0 ) {
                outputStream.write(buffer, 0, length);
            }
            return outputStream.toByteArray();
        } catch (IOException e) {
            log.error("unGzip error", e);
        }
        return null;
    }

}

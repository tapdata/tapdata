package com.tapdata.tm.proxy.utils;

import io.tapdata.modules.api.net.data.FileMeta;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.springframework.http.HttpHeaders;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/12 11:04 Create
 * @description
 */
public final class RemoteCallerUtil {
    private RemoteCallerUtil() {

    }

    public static void responseForFileMeta(FileMeta fileMeta, HttpServletResponse response, Logger log) throws IOException {
        response.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_OCTET_STREAM.getMimeType());
        response.setHeader(HttpHeaders.CONTENT_DISPOSITION, String.format("attachment; filename=%s", fileMeta.getFilename()));
        response.setHeader(HttpHeaders.CONTENT_LENGTH, String.valueOf(fileMeta.getFileSize()));
        response.setHeader("X-FileMeta-Code", fileMeta.getCode());
        try (InputStream inputStream = fileMeta.getFileInputStream();
             OutputStream outputStream = response.getOutputStream()) {
            long count = 0;
            int n;
            byte[] buffer = new byte[8192];
            while (-1 != (n = inputStream.read(buffer))) {
                outputStream.write(buffer, 0, n);
                count += n;
            }
            log.debug("Write file length {}", count);
        }
    }
}

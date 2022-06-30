package com.tapdata.tm.base.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletResponse;
import java.io.*;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/9/15 下午9:29
 * @description
 */
public class HttpServletResponseWrapper extends javax.servlet.http.HttpServletResponseWrapper{
    private Logger logger = LoggerFactory.getLogger(HttpServletResponseWrapper.class);
    private CachedHttpServletResponse outputStream;
    private String characterEncoding = "UTF-8";

    /**
     * Constructs a response adaptor wrapping the given response.
     *
     * @param response The response to be wrapped
     * @throws IllegalArgumentException if the response is null
     */
    public HttpServletResponseWrapper(HttpServletResponse response) {
        super(response);
        try {
            this.outputStream = new CachedHttpServletResponse(response.getOutputStream());
        } catch (IOException e) {
            this.outputStream = null;
            e.printStackTrace();
        }
    }

    public String getContentAsString() throws UnsupportedEncodingException {
        return outputStream != null ? outputStream.getContentAsString() : null;
    }

    @Override
    public ServletOutputStream getOutputStream() throws IOException {
        return outputStream != null ? outputStream : super.getOutputStream();
    }

    @Override
    public PrintWriter getWriter() throws IOException {
        return outputStream != null ? new PrintWriter(outputStream) : super.getWriter();
    }

    private class CachedHttpServletResponse extends ServletOutputStream {
        private final ServletOutputStream targetOutputStream;
        private ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        public CachedHttpServletResponse(ServletOutputStream targetOutputStream){
            this.targetOutputStream = targetOutputStream;
        }

        public String getContentAsString() throws UnsupportedEncodingException {
            return new String(outputStream.toByteArray(), characterEncoding);
        }

        @Override
        public void write(int b) throws IOException {
            this.outputStream.write(b);
            this.targetOutputStream.write(b);
        }

        @Override
        public void flush() throws IOException {
            try {
                super.flush();
                this.outputStream.flush();
            } catch (Exception e) {
                logger.error("Flush response data to client failed");
            }
            try {
                this.targetOutputStream.flush();
            } catch (Exception e) {
                logger.error("Flush response data to client failed");
            }
        }

        @Override
        public void close() throws IOException {
            super.close();
            this.outputStream.close();
            this.targetOutputStream.close();
        }

        @Override
        public boolean isReady() {
            return this.targetOutputStream.isReady();
        }

        @Override
        public void setWriteListener(WriteListener writeListener) {
            throw new UnsupportedOperationException();
        }
    }
}

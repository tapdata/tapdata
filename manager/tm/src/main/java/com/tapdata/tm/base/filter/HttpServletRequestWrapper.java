package com.tapdata.tm.base.filter;

import com.tapdata.tm.utils.GZIPUtil;
import org.apache.http.client.entity.GzipDecompressingEntity;
import org.springframework.util.Assert;
import org.springframework.util.StreamUtils;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import java.io.*;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/9/15 下午9:28
 * @description
 */
public class HttpServletRequestWrapper extends javax.servlet.http.HttpServletRequestWrapper {
    private byte[] content;
    private String characterEncoding = "UTF-8";
    private String contentEncoding = null;
    private static final ServletInputStream EMPTY_SERVLET_INPUT_STREAM =
            new DelegateServletInputStream(StreamUtils.emptyInput());
    private static final BufferedReader EMPTY_BUFFERED_READER =
            new BufferedReader(new StringReader(""));
    private ServletInputStream inputStream;
    private BufferedReader reader;

    /**
     * Constructs a request object wrapping the given request.
     *
     * @param request The request to wrap
     * @throws IllegalArgumentException if the request is null
     */
    public HttpServletRequestWrapper(HttpServletRequest request) {
        super(request);

        //if (request.getContentLengthLong() > 0){
            try {
                content = StreamUtils.copyToByteArray(request.getInputStream());
                contentEncoding = request.getHeader("content-encoding");
            } catch (IOException e) {
                e.printStackTrace();
            }
        if ("gzip".equals(contentEncoding)) {
            byte[] unGzipData = GZIPUtil.unGzip(this.content);
            if (unGzipData != null) {
                this.content = unGzipData;
            }
        } else if("deflate".equals(contentEncoding)) {
            //TODO: 实现 deflate 解压
        } else if("br".equals(contentEncoding)) {
            //TODO: 实现 br 解压
        } else if("compress".equals(contentEncoding)) {
            //TODO: 实现 compress 解压
        }
        //}
    }

    public String getContentAsString() throws IllegalStateException, UnsupportedEncodingException {
        Assert.state(this.characterEncoding != null,
                "Cannot get content as a String for a null character encoding. " +
                        "Consider setting the characterEncoding in the request.");

        if (this.content == null) {
            return null;
        }
        return new String(this.content, this.characterEncoding);
    }

    @Override
    public ServletInputStream getInputStream() throws IOException {
        if (this.inputStream != null) {
            return this.inputStream;
        } else if (this.reader != null) {
            throw new IllegalStateException(
                    "Cannot call getInputStream() after getReader() has already been called for the current request")			;
        }
        this.inputStream = (this.content != null ?
                new DelegateServletInputStream(new ByteArrayInputStream(this.content)) :
                EMPTY_SERVLET_INPUT_STREAM);
        return this.inputStream;
    }

    @Override
    public BufferedReader getReader() throws IOException {
        if (this.reader != null) {
            return this.reader;
        } else if (this.inputStream != null) {
            throw new IllegalStateException(
                    "Cannot call getReader() after getInputStream() has already been called for the current request")			;
        }
        if (this.content != null) {
            InputStream sourceStream = new ByteArrayInputStream(this.content);
            Reader sourceReader = (this.characterEncoding != null) ?
                    new InputStreamReader(sourceStream, this.characterEncoding) :
                    new InputStreamReader(sourceStream);
            this.reader = new BufferedReader(sourceReader);
        }
        else {
            this.reader = EMPTY_BUFFERED_READER;
        }
        return this.reader;
    }

    private static class DelegateServletInputStream extends ServletInputStream {
        private final InputStream sourceStream;

        private boolean finished = false;


        public DelegateServletInputStream(InputStream sourceStream) {
            Assert.notNull(sourceStream, "Source InputStream must not be null");
            this.sourceStream = sourceStream;
        }

        public final InputStream getSourceStream() {
            return this.sourceStream;
        }


        @Override
        public int read() throws IOException {
            int data = this.sourceStream.read();
            if (data == -1) {
                this.finished = true;
            }
            return data;
        }

        @Override
        public int available() throws IOException {
            return this.sourceStream.available();
        }

        @Override
        public void close() throws IOException {
            super.close();
            this.sourceStream.close();
        }

        @Override
        public boolean isFinished() {
            return this.finished;
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setReadListener(ReadListener readListener) {
            throw new UnsupportedOperationException();
        }
    }
}

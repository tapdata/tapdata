package io.tapdata.connector.doris.streamload;

/**
 * @Author dayun
 * @Date 7/14/22
 */

import io.tapdata.entity.logger.TapLogger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Record Stream for writing record.
 */
public class RecordStream extends InputStream {
    private final RecordBuffer recordBuffer;
    private long contentLength;

    public RecordStream(int bufferSize, int bufferCount) {
        this.recordBuffer = new RecordBuffer(bufferSize, bufferCount);
    }

    public void startInput() {
        recordBuffer.startBufferData();
    }

    public void endInput() throws IOException {
        recordBuffer.stopBufferData();
    }

    @Override
    public int read() throws IOException {
        try {
            return recordBuffer.read();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int read(byte[] buff) throws IOException {
        try {
            return recordBuffer.read(buff);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        try {
            return recordBuffer.read(b, off, len);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void write(byte[] buff) throws IOException {
        try {
            recordBuffer.write(buff);
            contentLength += buff.length;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void init() {
        recordBuffer.init();
    }

    public void setContentLength(long contentLength) {
        this.contentLength = contentLength;
    }

    public long getContentLength() {
        return contentLength;
    }

    public boolean canWrite(int length) {
        return recordBuffer.currentBufferRemaining() > length;
    }

    @Override
    public String toString() {
        if (null != recordBuffer.currentWriteBuffer) {
            return new String(recordBuffer.currentWriteBuffer.array(), StandardCharsets.UTF_8);
        }
        return "";
    }
}

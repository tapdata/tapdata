package io.tapdata.connector.doris.streamload;

/**
 * @Author dayun
 * @Date 7/14/22
 */

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Record Stream for writing record.
 */
public class RecordStream extends InputStream {
    private final RecordBuffer recordBuffer;

    @Override
    public int read() throws IOException {
        try {
            return recordBuffer.read();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

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
    public int read(byte[] buff) throws IOException {
        try {
            return recordBuffer.read(buff);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void write(byte[] buff) throws IOException {
        try {
            recordBuffer.write(buff);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void init() {
        recordBuffer.init();
    }

    public boolean canWrite(int length) {
        return recordBuffer.currentBufferRemaining() > length;
    }

    @Override
    public String toString() {
        if (null == recordBuffer.currentWriteBuffer) {
            return "";
        }
        return new String(recordBuffer.currentWriteBuffer.array(), StandardCharsets.UTF_8);
    }
}

package io.tapdata.connector.doris.streamload;

import io.tapdata.entity.logger.TapLogger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @Author dayun
 * @Date 7/14/22
 */
public class RecordBuffer {
    private static final String TAG = RecordBuffer.class.getSimpleName();
    BlockingQueue<ByteBuffer> writeQueue;
    BlockingQueue<ByteBuffer> readQueue;
    int bufferCapacity;
    int queueSize;
    ByteBuffer currentWriteBuffer;
    ByteBuffer currentReadBuffer;

    public RecordBuffer(int capacity, int queueSize) {
        TapLogger.info(TAG, "init RecordBuffer capacity {}, count {}", capacity, queueSize);
        assert capacity > 0;
        assert queueSize > 1;
        this.bufferCapacity = capacity;
        this.queueSize = queueSize;
        init();
    }

    public void init() {
        this.writeQueue = new ArrayBlockingQueue<>(queueSize);
        for (int index = 0; index < queueSize; index++) {
            this.writeQueue.add(ByteBuffer.allocate(bufferCapacity));
        }
        readQueue = new LinkedBlockingDeque<>();
    }

    public void startBufferData() {
        assert readQueue.size() == 0;
        assert writeQueue.size() == queueSize;
        for (ByteBuffer byteBuffer: writeQueue) {
            assert byteBuffer.position() == 0;
            assert byteBuffer.remaining() == bufferCapacity;
        }
    }

    public void stopBufferData() throws IOException{
        try {
            // add Empty buffer as finish flag.
            boolean isEmpty = false;
            if (currentWriteBuffer != null) {
                currentWriteBuffer.flip();
                // check if the current write buffer is empty.
                isEmpty = currentWriteBuffer.limit() == 0;
                readQueue.put(currentWriteBuffer);
                currentWriteBuffer = null;
            }
            if (!isEmpty) {
                ByteBuffer byteBuffer = writeQueue.take();
                byteBuffer.flip();
                assert byteBuffer.limit() == 0;
                readQueue.put(byteBuffer);
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public void write(byte[] buf) throws InterruptedException{
        int wPos = 0;
        do {
            if (currentWriteBuffer == null) {
                currentWriteBuffer = writeQueue.take();
            }
            int available = currentWriteBuffer.remaining();
            int nWrite = Math.min(available, buf.length - wPos);
            currentWriteBuffer.put(buf, wPos, nWrite);
            wPos += nWrite;
            if (currentWriteBuffer.remaining() == 0) {
                currentWriteBuffer.flip();
                readQueue.put(currentWriteBuffer);
                currentWriteBuffer = null;
            }
        } while (wPos != buf.length);
    }

    private void getCurrentReadBufferIfNeed() throws InterruptedException {
        if (currentReadBuffer == null) {
            currentReadBuffer = readQueue.take();
        }
    }

    public int read() throws InterruptedException {
        getCurrentReadBufferIfNeed();
        // add empty buffer as end flag
        if (currentReadBuffer.limit() == 0) {
            recycleBuffer(currentReadBuffer);
            currentReadBuffer = null;
            assert readQueue.size() == 0;
            return -1;
        }
        int _byte = currentReadBuffer.get();
        if (currentReadBuffer.remaining() == 0) {
            recycleBuffer(currentReadBuffer);
            currentReadBuffer = null;
        }
        return _byte;
    }

    public int read(byte[] buf) throws InterruptedException {
        getCurrentReadBufferIfNeed();
        // add empty buffer as end flag
        if (currentReadBuffer.limit() == 0) {
            recycleBuffer(currentReadBuffer);
            currentReadBuffer = null;
            assert readQueue.size() == 0;
            return -1;
        }
        int available = currentReadBuffer.remaining();
        int nRead = Math.min(available, buf.length);
        currentReadBuffer.get(buf, 0, nRead);
        if (currentReadBuffer.remaining() == 0) {
            recycleBuffer(currentReadBuffer);
            currentReadBuffer = null;
        }
        return nRead;
    }

    public int read(byte[] b, int off, int len) throws InterruptedException {
        getCurrentReadBufferIfNeed();
        if (currentReadBuffer.limit() == 0) {
            recycleBuffer(currentReadBuffer);
            currentReadBuffer = null;
            return -1;
        }
        currentReadBuffer.get(b, off, len);
        if (currentReadBuffer.remaining() == 0) {
            recycleBuffer(currentReadBuffer);
            currentReadBuffer = null;
            getCurrentReadBufferIfNeed();
            if (currentReadBuffer.limit() == 0) {
                recycleBuffer(currentReadBuffer);
                currentReadBuffer = null;
            }
        }
        return b.length;
    }

    private void recycleBuffer(ByteBuffer buffer) throws InterruptedException {
        buffer.clear();
        writeQueue.put(buffer);
    }

    public int currentBufferRemaining() {
        return null == currentWriteBuffer ? Constants.CACHE_BUFFER_SIZE : currentWriteBuffer.remaining();
    }

    public int getWriteQueueSize() {
        return writeQueue.size();
    }

    public int getReadQueueSize() {
        return readQueue.size();
    }
}

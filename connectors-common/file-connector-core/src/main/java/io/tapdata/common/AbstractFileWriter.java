package io.tapdata.common;

import io.tapdata.file.TapFileStorage;
import io.tapdata.kit.ErrorKit;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

public abstract class AbstractFileWriter implements AutoCloseable {

    protected String path;
    protected OutputStream outputStream;
    protected Writer writer;
    protected final TapFileStorage storage;
    protected final String fileEncoding;
    protected boolean closed = true;

    public AbstractFileWriter(TapFileStorage storage, String path, String fileEncoding) throws Exception {
        this.path = path;
        this.storage = storage;
        this.fileEncoding = fileEncoding;
        init();
    }

    public void init() throws Exception {
        this.outputStream = storage.openFileOutputStream(path, true);
        this.writer = new OutputStreamWriter(outputStream, fileEncoding);
        closed = false;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public OutputStream getOutputStream() {
        return outputStream;
    }

    public void setOutputStream(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    public Writer getWriter() {
        return writer;
    }

    public void setWriter(Writer writer) {
        this.writer = writer;
    }

    public boolean isClosed() {
        return closed;
    }

    public void close() {
        ErrorKit.ignoreAnyError(() -> writer.close());
        ErrorKit.ignoreAnyError(() -> outputStream.close());
        closed = true;
    }

    public abstract void flush();
}

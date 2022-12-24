package io.tapdata.connector.csv.writer;

import com.opencsv.CSVWriter;
import io.tapdata.file.TapFileStorage;
import io.tapdata.kit.ErrorKit;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.function.Consumer;

public class CsvFileWriter {

    private String path;
    private OutputStream outputStream;
    private CSVWriter csvWriter;
    private Writer writer;
    private final TapFileStorage storage;
    private final String fileEncoding;
    private boolean closed = true;

    public CsvFileWriter(TapFileStorage storage, String path, String fileEncoding) throws Exception {
        this.path = path;
        this.storage = storage;
        this.fileEncoding = fileEncoding;
        init();
    }

    public void init() throws Exception {
        this.outputStream = storage.openFileOutputStream(path,true);
        this.writer = new OutputStreamWriter(outputStream, fileEncoding);
        this.csvWriter = new CSVWriter(writer);
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

    public CSVWriter getCsvWriter() {
        return csvWriter;
    }

    public void setCsvWriter(CSVWriter csvWriter) {
        this.csvWriter = csvWriter;
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
        ErrorKit.ignoreAnyError(() -> csvWriter.close());
        ErrorKit.ignoreAnyError(() -> writer.close());
        ErrorKit.ignoreAnyError(() -> outputStream.close());
        closed = true;
    }

    public void flush() {
        ErrorKit.ignoreAnyError(() -> csvWriter.flush());
    }

}

package io.tapdata.connector.csv.writer;

import com.opencsv.CSVWriter;
import io.tapdata.common.AbstractFileWriter;
import io.tapdata.file.TapFileStorage;
import io.tapdata.kit.ErrorKit;

public class CsvFileWriter extends AbstractFileWriter {

    private CSVWriter csvWriter;
    private char separator;
    private char quoteChar;
    private String lineEnd;

    public CsvFileWriter(TapFileStorage storage, String path, String fileEncoding) throws Exception {
        super(storage, path, fileEncoding);
    }

    @Override
    public void init() throws Exception {
        super.init();
        this.csvWriter = new CSVWriter(writer, separator, quoteChar, quoteChar, lineEnd);
    }

    public void setRule(char separator, char quoteChar, String lineEnd) {
        this.separator = separator;
        this.quoteChar = quoteChar;
        this.lineEnd = lineEnd;
    }

    public CSVWriter getCsvWriter() {
        return csvWriter;
    }

    public void setCsvWriter(CSVWriter csvWriter) {
        this.csvWriter = csvWriter;
    }

    @Override
    public void close() {
        ErrorKit.ignoreAnyError(() -> csvWriter.close());
        super.close();
    }

    @Override
    public void flush() {
        ErrorKit.ignoreAnyError(() -> csvWriter.flush());
    }

}

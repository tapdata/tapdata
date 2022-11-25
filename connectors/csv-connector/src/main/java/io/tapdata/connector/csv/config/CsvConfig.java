package io.tapdata.connector.csv.config;

import io.tapdata.common.FileConfig;

public class CsvConfig extends FileConfig {

    private String delimiter;

    public CsvConfig() {
        setFileType("csv");
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

}

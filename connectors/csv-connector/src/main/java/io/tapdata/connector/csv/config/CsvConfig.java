package io.tapdata.connector.csv.config;

import io.tapdata.common.FileConfig;

public class CsvConfig extends FileConfig {

    private Boolean offStandard = false;
    private String lineExpression;
    private String separator = ",";
    private String quoteChar = "\"";
    private String lineEnd = "\n";

    public CsvConfig() {
        setFileType("csv");
    }

    public Boolean getOffStandard() {
        return offStandard;
    }

    public void setOffStandard(Boolean offStandard) {
        this.offStandard = offStandard;
    }

    public String getLineExpression() {
        return lineExpression;
    }

    public void setLineExpression(String lineExpression) {
        this.lineExpression = lineExpression;
    }

    public String getSeparator() {
        return separator;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }

    public String getQuoteChar() {
        return quoteChar;
    }

    public void setQuoteChar(String quoteChar) {
        this.quoteChar = quoteChar;
    }

    public String getLineEnd() {
        return lineEnd;
    }

    public void setLineEnd(String lineEnd) {
        this.lineEnd = lineEnd;
    }
}

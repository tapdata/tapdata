package io.tapdata.connector.csv.config;

import io.tapdata.common.FileConfig;

public class CsvConfig extends FileConfig {

    private Boolean offStandard = false;
    private String lineExpression;

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
}

package io.tapdata.connector.excel.config;

import io.tapdata.common.FileConfig;

public class ExcelConfig extends FileConfig {

    private String excelPassword;
    private String sheetLocation;
    private String colLocation;

    public ExcelConfig() {
        setFileType("excel");
    }

    public String getExcelPassword() {
        return excelPassword;
    }

    public void setExcelPassword(String excelPassword) {
        this.excelPassword = excelPassword;
    }

    public String getSheetLocation() {
        return sheetLocation;
    }

    public void setSheetLocation(String sheetLocation) {
        this.sheetLocation = sheetLocation;
    }

    public String getColLocation() {
        return colLocation;
    }

    public void setColLocation(String colLocation) {
        this.colLocation = colLocation;
    }
}

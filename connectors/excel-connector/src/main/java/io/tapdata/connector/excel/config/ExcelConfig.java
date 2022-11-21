package io.tapdata.connector.excel.config;

import io.tapdata.common.FileConfig;
import io.tapdata.connector.excel.util.ExcelUtil;
import io.tapdata.kit.EmptyKit;

import java.util.Map;
import java.util.Set;

public class ExcelConfig extends FileConfig {

    private String excelPassword;
    private String sheetLocation;
    private Set<Integer> sheetNum;
    private String colLocation;
    private Integer firstColumn;
    private Integer lastColumn;

    public ExcelConfig() {
        setFileType("excel");
    }

    public ExcelConfig load(Map<String, Object> map) {
        super.load(map);
        if (EmptyKit.isNotBlank(sheetLocation)) {
            sheetNum = ExcelUtil.getSheetNumber(sheetLocation);
        }
        if (EmptyKit.isNotBlank(colLocation)) {
            String[] arr = colLocation.split("~");
            firstColumn = Integer.parseInt(arr[0]);
            lastColumn = Integer.parseInt(arr[arr.length - 1]);
        }
        return this;
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

    public Set<Integer> getSheetNum() {
        return sheetNum;
    }

    public void setSheetNum(Set<Integer> sheetNum) {
        this.sheetNum = sheetNum;
    }

    public Integer getFirstColumn() {
        return firstColumn;
    }

    public void setFirstColumn(Integer firstColumn) {
        this.firstColumn = firstColumn;
    }

    public Integer getLastColumn() {
        return lastColumn;
    }

    public void setLastColumn(Integer lastColumn) {
        this.lastColumn = lastColumn;
    }
}

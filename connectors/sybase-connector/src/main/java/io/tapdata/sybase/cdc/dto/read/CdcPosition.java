package io.tapdata.sybase.cdc.dto.read;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author GavinXiao
 * @description CdcPosition create by Gavin
 * @create 2023/7/12 18:54
 **/
public class CdcPosition {
    Map<String, PositionOffset> tableOffset;

    public CdcPosition() {
        tableOffset = new LinkedHashMap<>();
    }

    public CdcPosition(String tableName, PositionOffset positionOffset) {
        tableOffset = new LinkedHashMap<>();
        tableOffset.put(tableName, positionOffset);
    }

    public CdcPosition(Map<String, PositionOffset> positionOffsetMap) {
        tableOffset = Optional.ofNullable(positionOffsetMap).orElse(new LinkedHashMap<>());
    }

    public PositionOffset get(String tableName) {
        return null == tableOffset ? null : tableOffset.get(tableName);
    }

    public void add(String tableName, PositionOffset offset) {
        tableOffset.put(tableName, offset);
    }

    public static class PositionOffset {
        //private String fileName;
        Map<String, CSVOffset> csvFile;

        public PositionOffset() {
            csvFile = new LinkedHashMap<>();
        }

        public Map<String, CSVOffset> getCsvFile() {
            return csvFile;
        }

        public void setCsvFile(Map<String, CSVOffset> csvFile) {
            this.csvFile = csvFile;
        }

        public CSVOffset csvOffset(String fileName) {
            return null == csvFile ? null : csvFile.get(fileName);
        }

        public void csvOffset(String fileName, CSVOffset offset) {
            csvFile.put(fileName, offset);
        }
    }

    public static class CSVOffset {
        private int line;
        private boolean isOver;

        public int getLine() {
            return line;
        }

        public void setLine(int line) {
            this.line = line;
        }

        public boolean isOver() {
            return isOver;
        }

        public void setOver(boolean over) {
            isOver = over;
        }
    }

    public Map<String, PositionOffset> getTableOffset() {
        return tableOffset;
    }

    public void setTableOffset(Map<String, PositionOffset> tableOffset) {
        this.tableOffset = tableOffset;
    }
}

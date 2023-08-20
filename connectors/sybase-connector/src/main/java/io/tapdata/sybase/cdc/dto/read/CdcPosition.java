package io.tapdata.sybase.cdc.dto.read;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author GavinXiao
 * @description CdcPosition create by Gavin
 * @create 2023/7/12 18:54
 **/
public class CdcPosition implements Serializable {
    private static final long serialVersionUID = 1L;

    Map<String, Map<String, Map<String, PositionOffset>>> tableOffset;
    long cdcStartTime;

    public CdcPosition() {
        tableOffset = new LinkedHashMap<>();
    }

    public CdcPosition cdcStartTime(long cdcStartTime) {
        this.cdcStartTime = cdcStartTime;
        return this;
    }

    public long getCdcStartTime() {
        return cdcStartTime;
    }

    public void setCdcStartTime(long cdcStartTime) {
        this.cdcStartTime = cdcStartTime;
    }

    public CdcPosition(String database, String schema, String tableName, PositionOffset positionOffset) {
        tableOffset = new HashMap<>();
        Map<String, Map<String, PositionOffset>> schemaMap = new HashMap<>();
        tableOffset.put(database, schemaMap);
        schemaMap.put(schema, new LinkedHashMap<>());
        schemaMap.get(schema).put(tableName, positionOffset);
    }

    public CdcPosition(Map<String, Map<String, Map<String, PositionOffset>>> positionOffsetMap) {
        tableOffset = Optional.ofNullable(positionOffsetMap).orElse(new LinkedHashMap<>());
    }

    public PositionOffset get(String database, String schema, String tableName) {
        if (null == tableOffset) return null;
        Map<String, Map<String, PositionOffset>> databaseMap = tableOffset.get(database);
        if (null == databaseMap) return null;
        Map<String, PositionOffset> schemaMap = databaseMap.get(schema);
        if (null == schemaMap) return null;
        return schemaMap.get(tableName);
    }

    public void add(String database, String schema, String tableName, PositionOffset offset) {
        if (null == tableOffset) tableOffset = new HashMap<>();
        Map<String, Map<String, PositionOffset>> databaseMap = tableOffset.computeIfAbsent(database, k -> new HashMap<>());
        Map<String, PositionOffset> schemaMap = databaseMap.computeIfAbsent(schema, k -> new LinkedHashMap<>());
        schemaMap.put(tableName, offset);
    }

    public static class PositionOffset implements Serializable {
        public static final long serialVersionUID = 2L;
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

    public static class CSVOffset implements Serializable {
        public static final long serialVersionUID = 3L;
        private int line;
        private boolean isOver;

        public int getLine() {
            return line;
        }

        public int addAndGet() {
            this.line++;
            return this.line;
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

    public Map<String, Map<String, Map<String, PositionOffset>>> getTableOffset() {
        return tableOffset;
    }

    public void setTableOffset(Map<String, Map<String, Map<String, PositionOffset>>> tableOffset) {
        this.tableOffset = tableOffset;
    }
}

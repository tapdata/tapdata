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
    boolean isMultiTask;

    public CdcPosition() {
        tableOffset = new LinkedHashMap<>();
        isMultiTask = true;
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
        Map<Integer, CSVOffset> csvFile;
        String pathSuf;

        public String getPathSuf() {
            return this.pathSuf;
        }

        public PositionOffset(String pathSuf) {
            this.pathSuf = pathSuf;
            csvFile = new LinkedHashMap<>();
        }

        public Map<Integer, CSVOffset> getCsvFile() {
            return csvFile;
        }

        public void setCsvFile(Map<Integer, CSVOffset> csvFile) {
            this.csvFile = csvFile;
        }

        public CSVOffset csvOffset(String fileName) {
            return null == csvFile ? null : csvFile.get(fixFileNameByFilePath(fileName));
        }

        public CSVOffset csvOffsetByFullPath(String fullPath) {
            Integer fileNameIndex = fixFileNameByFilePath(fullPath);
            return null == csvFile || null == fileNameIndex || fileNameIndex < 0 ? null : csvFile.get(fileNameIndex);
        }

        public void csvOffset(Integer fileIndex, CSVOffset offset) {
            csvFile.put(fileIndex, offset);
        }

        public void csvOffset(String fileName, CSVOffset offset) {
            csvFile.put(fixFileNameByFilePath(fileName), offset);
        }

        public void csvOffsetByFullPath(String fullPath, CSVOffset offset) {
            csvFile.put(fixFileNameByFilePath(fullPath), offset);
        }

        /**
         * @deprecated
         * */
//        public String fixFileNameByFilePath(String path) {
//            return null == path || "".equals(path.trim()) ? null : path.replace(pathSuf, "");
//        }

        public Integer fixFileNameByFilePath(String path) {
            String fileName = null == path || "".equals(path.trim()) ? null : path.replace(pathSuf, "");;
            return fixFileNameByFilePathWithoutSuf(fileName);
        }

        public static Integer fixFileNameByFilePathWithoutSuf(String fileName) {
            if (null == fileName) {
                return -1;
            }
            String[] split = fileName.split("\\.");
            if( split.length != 5) {
                return  -1;
            }
            String indexFormat = split[3];
            if (null == indexFormat || !indexFormat.startsWith("part_")){
                return -1;
            }
            String partIndexStr = indexFormat.replace("part_", "");
            try {
                return Integer.parseInt(partIndexStr);
            } catch (Exception e) {
                return -1;
            }
        }

        public String parseFileName(String database, String schema, String table, Integer index) {
            return pathSuf + database + "." + schema + "." + table + ".part_" +  index + ".csv";
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
        public int addAndGet(int nums) {
            this.line += nums;
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

    public boolean isMultiTask() {
        return isMultiTask;
    }

    public void setMultiTask(boolean multiTask) {
        isMultiTask = multiTask;
    }

    public CdcPosition notMultiTask() {
        this.isMultiTask = false;
        return this;
    }
}

package io.tapdata.common;

import io.tapdata.file.TapFile;

import java.util.Map;

public class FileOffset {

    private String path;
    private int sheetNum;
    private int dataLine;
    private Map<String, TapFile> allFiles;

    public FileOffset() {

    }

    public FileOffset(String path, int dataLine) {
        this.path = path;
        this.dataLine = dataLine;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public int getSheetNum() {
        return sheetNum;
    }

    public void setSheetNum(int sheetNum) {
        this.sheetNum = sheetNum;
    }

    public int getDataLine() {
        return dataLine;
    }

    public void setDataLine(int dataLine) {
        this.dataLine = dataLine;
    }

    public Map<String, TapFile> getAllFiles() {
        return allFiles;
    }

    public void setAllFiles(Map<String, TapFile> allFiles) {
        this.allFiles = allFiles;
    }
}

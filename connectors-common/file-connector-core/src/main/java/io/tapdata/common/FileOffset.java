package io.tapdata.common;

import java.util.Map;

public class FileOffset {

    private String path;
    private int dataLine;
    private Map<String, Long> allLastModified;

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

    public int getDataLine() {
        return dataLine;
    }

    public void setDataLine(int dataLine) {
        this.dataLine = dataLine;
    }

    public Map<String, Long> getAllLastModified() {
        return allLastModified;
    }

    public void setAllLastModified(Map<String, Long> allLastModified) {
        this.allLastModified = allLastModified;
    }
}

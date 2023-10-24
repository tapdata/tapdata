package io.tapdata.file;

import java.util.Map;

public class TapFile {
    public static final int TYPE_FILE = 1;
    public static final int TYPE_DIRECTORY = 2;
    private Integer type;

    public TapFile type(Integer type) {
        this.type = type;
        return this;
    }

    private String name;

    public TapFile name(String name) {
        this.name = name;
        return this;
    }

    private Long lastModified;

    public TapFile lastModified(Long lastModified) {
        this.lastModified = lastModified;
        return this;
    }

    private String path;

    public TapFile path(String path) {
        this.path = path;
        return this;
    }

    private String contentType;

    public TapFile contentType(String contentType) {
        this.contentType = contentType;
        return this;
    }

    private Long length;

    public TapFile length(Long length) {
        this.length = length;
        return this;
    }

    private Map<String, Object> metadata;

    public TapFile metadata(Map<String, Object> metadata) {
        this.metadata = metadata;
        return this;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getLastModified() {
        return lastModified;
    }

    public void setLastModified(Long lastModified) {
        this.lastModified = lastModified;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public Long getLength() {
        return length;
    }

    public void setLength(Long length) {
        this.length = length;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    @Override
    public String toString() {
        return "TapFile{" + "path='" + path + '\'' +
                ", type=" + (type == TYPE_FILE ? "FILE" : "DIRECTORY") +
                ", size='" + length + '\'' +
                ", lastModified=" + lastModified +
                '}';
    }

    @Override
    public int hashCode() {
        return this.path.hashCode() + this.type.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        if (obj instanceof TapFile) {
            TapFile f = (TapFile) obj;
            return f.type.equals(this.type) && f.path.equals(this.path);
        }
        return false;
    }
}
package io.tapdata.pdk.apis.charset;

public class DatabaseCharset {
    public static DatabaseCharset create() {
        return new DatabaseCharset();
    }
    private String charset;
    public DatabaseCharset charset(String charset) {
        this.charset = charset;
        return this;
    }
    private String description;
    public DatabaseCharset description(String description) {
        this.description = description;
        return this;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}

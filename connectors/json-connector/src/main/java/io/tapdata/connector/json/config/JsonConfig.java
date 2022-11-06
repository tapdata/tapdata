package io.tapdata.connector.json.config;

import io.tapdata.common.FileConfig;

public class JsonConfig extends FileConfig {

    private String jsonType;

    public JsonConfig() {
        setFileType("json");
    }

    public String getJsonType() {
        return jsonType;
    }

    public void setJsonType(String jsonType) {
        this.jsonType = jsonType;
    }
}

package io.tapdata.supervisor.convert.entity;

import java.util.Map;
import java.util.Optional;

public class WException extends WBase implements Resolvable<WException>{
    private String path;
    private String name;

    @Override
    public WException parser(Map<String, Object> parserMap) {
        super.parser(parserMap);
        if (this.ignore) return null;
        this.name = (String) Optional.ofNullable(parserMap.get(WZTags.W_NAME)).orElse(WZTags.DEFAULT_EMPTY);
        this.path = (String) Optional.ofNullable(parserMap.get(WZTags.W_PATH)).orElse(WZTags.DEFAULT_EMPTY);
        return this;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}

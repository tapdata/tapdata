package io.tapdata.supervisor.convert.entity;

import java.util.Map;

class WBase {
    protected boolean ignore;

    public boolean isIgnore() {
        return ignore;
    }

    public void setIgnore(boolean ignore) {
        this.ignore = ignore;
    }

    public WBase parser(Map<String, Object> parserMap) {
        this.ignore = WZTags.toBoolean(parserMap,WZTags.W_IGNORE,false);
        return this;
    }
}

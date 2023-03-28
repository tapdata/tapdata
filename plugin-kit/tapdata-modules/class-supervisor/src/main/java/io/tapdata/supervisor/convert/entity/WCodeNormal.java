package io.tapdata.supervisor.convert.entity;

import java.util.Map;

class WCodeNormal extends WCode {

    @Override
    public WCodeNormal parser(Map<String, Object> parserMap) {
        super.parser(parserMap);
        return this;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public boolean isAppendIs() {
        return appendIs;
    }

    public void setAppendIs(boolean appendIs) {
        this.appendIs = appendIs;
    }
}

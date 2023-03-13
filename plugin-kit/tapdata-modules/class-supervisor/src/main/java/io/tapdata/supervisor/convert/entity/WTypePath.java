package io.tapdata.supervisor.convert.entity;

import java.util.Map;

class WTypePath extends WBaseTarget {
    @Override
    public WTypePath parser(Map<String, Object> parserMap) {
        super.parser(parserMap);
        return this;
    }

    @Override
    public String[] freshenPaths() {
        return new String[]{this.path};
    }
}

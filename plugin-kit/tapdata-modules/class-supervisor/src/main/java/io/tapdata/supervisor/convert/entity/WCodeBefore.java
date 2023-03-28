package io.tapdata.supervisor.convert.entity;

import java.util.Map;

class WCodeBefore extends WCode{

    @Override
    public WCodeBefore parser(Map<String, Object> parserMap) {
        super.parser(parserMap);
        return this;
    }

}

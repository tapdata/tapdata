package io.tapdata.supervisor.convert.entity;

import io.tapdata.supervisor.utils.ClassUtil;

import java.util.Map;

class WTypePath extends WBaseTarget {
    public WTypePath(String savePath,String jarFilePath) {
        super(savePath, jarFilePath);
    }

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

package io.tapdata.supervisor.convert.entity;

import java.util.Map;

interface Resolvable<T> {
    public T parser(Map<String,Object> parserMap);
}

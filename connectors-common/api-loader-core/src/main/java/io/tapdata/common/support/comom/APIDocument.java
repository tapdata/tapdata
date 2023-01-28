package io.tapdata.common.support.comom;

import java.util.Map;

public interface APIDocument<V> {
    public V analysis(Map<String, Object> apiJson,Map<String, Object> params);
}

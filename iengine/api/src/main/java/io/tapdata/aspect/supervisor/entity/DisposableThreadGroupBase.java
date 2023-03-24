package io.tapdata.aspect.supervisor.entity;

import io.tapdata.entity.utils.DataMap;

public abstract class DisposableThreadGroupBase {
    public static final String MODE_KEY = "mode";
    public DataMap summary(){
        return new DataMap().kv(MODE_KEY,"default");
    }
}

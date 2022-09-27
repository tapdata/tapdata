package io.tapdata.zoho.service.commandMode;

import io.tapdata.zoho.entity.CommandResultV2;

import java.util.Map;

public abstract class ConfigContextChecker<T> {
    protected abstract boolean checkerConfig(Map<String,Object> context);
    protected abstract CommandResultV2 command(T entity);
}

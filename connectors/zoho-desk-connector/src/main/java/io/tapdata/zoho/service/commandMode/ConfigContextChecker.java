package io.tapdata.zoho.service.commandMode;

import io.tapdata.zoho.entity.CommandResultV2;

import java.util.Map;

public abstract class ConfigContextChecker<T> {
    protected String language;
    protected abstract boolean checkerConfig(Map<String,Object> context);

    protected abstract CommandResultV2 commandResult(T entity);

    public String language(){
        return this.language;
    }
    public void language(String language){
        this.language = language;
    }
}

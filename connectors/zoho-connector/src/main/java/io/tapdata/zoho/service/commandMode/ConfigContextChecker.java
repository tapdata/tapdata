package io.tapdata.zoho.service.commandMode;

import java.util.Map;

public abstract class ConfigContextChecker {
    protected abstract boolean checkerConfig(Map<String,Object> context);
}

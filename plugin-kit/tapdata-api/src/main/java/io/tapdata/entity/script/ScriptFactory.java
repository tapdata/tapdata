package io.tapdata.entity.script;

import javax.script.ScriptEngine;

public interface ScriptFactory {

    String TYPE_JAVASCRIPT = "javascript";
    String TYPE_PYTHON = "python";

    ScriptEngine create(String type, ScriptOptions scriptOptions);
}

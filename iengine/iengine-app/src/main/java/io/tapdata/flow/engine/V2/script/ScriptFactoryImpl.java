package io.tapdata.flow.engine.V2.script;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.utils.InstanceFactory;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

@Implementation(ScriptFactory.class)
public class ScriptFactoryImpl implements ScriptFactory {
    public static void main(String[] args) {
        ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class);
        ScriptEngine engine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, null);
        try {
            engine.eval("alert('aaa')");
        } catch (ScriptException e) {
            throw new RuntimeException(e);
        }
        if (engine instanceof Invocable) {
            Invocable invocable = (Invocable) engine;
            try {
                invocable.invokeFunction("main", "hello");
            } catch (ScriptException | NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public ScriptEngine create(String type, ScriptOptions scriptOptions) {
        switch (type) {
            case TYPE_PYTHON:
                break;
            case TYPE_JAVASCRIPT:
                return new TapJavaScriptEngine(scriptOptions);
        }
        return null;
    }
}

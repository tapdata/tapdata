package io.tapdata.flow.engine.V2.script;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.error.TapAPIErrorCodes;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.utils.InstanceFactory;
import org.apache.commons.lang3.exception.ExceptionUtils;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.lang.reflect.InvocationTargetException;

@Implementation(ScriptFactory.class)
public class ScriptFactoryImpl implements ScriptFactory {
    public static void main(String[] args) {
        ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class);
        ScriptEngine engine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions().customEngine(ScriptFactory.TYPE_JAVASCRIPT, TapJavaScriptEngine.class));
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
                Class<? extends ScriptEngine> engineClass = scriptOptions.getEngineCustomClass(type);
                if(engineClass != null) {
                    try {
                        return engineClass.getConstructor(ScriptOptions.class).newInstance();
                    } catch (Throwable e) {
                        throw new CoreException(TapAPIErrorCodes.ERROR_INSTANTIATE_ENGINE_CLASS_FAILED, e, "Instantiate engine class {} failed, {}", engineClass, e.getMessage());
                    }
                }
                return new TapJavaScriptEngine(scriptOptions);
        }
        return null;
    }
}

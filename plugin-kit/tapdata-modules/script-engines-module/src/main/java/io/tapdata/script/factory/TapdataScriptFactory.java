package io.tapdata.script.factory;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.error.TapAPIErrorCodes;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.script.factory.py.TapPythonEngine;
import io.tapdata.script.factory.script.TapRunScriptEngine;

import javax.script.ScriptEngine;

/**
 * @author aplomb
 */
@Implementation(value = ScriptFactory.class, type = "tapdata")
public class TapdataScriptFactory implements ScriptFactory {
    public static void main(String[] args) {
        ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class, "tapdata");
        ScriptEngine javaScriptEngine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions());
        ScriptEngine pythonEngine = scriptFactory.create(ScriptFactory.TYPE_PYTHON, new ScriptOptions());
		try {
			pythonEngine.eval("import java.util.ArrayList as ArrayList");
		}catch (Exception e){}

		try {
			pythonEngine.eval("import requests;");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

    @Override
    public ScriptEngine create(String type, ScriptOptions scriptOptions) {
		Class<? extends ScriptEngine> engineClass = scriptOptions.getEngineCustomClass(type);
		if(engineClass != null) {
			try {
				return engineClass.getConstructor(ScriptOptions.class).newInstance(scriptOptions);
			} catch (Throwable e) {
				throw new CoreException(TapAPIErrorCodes.ERROR_INSTANTIATE_ENGINE_CLASS_FAILED, e, "Instantiate engine class {} failed, {}", engineClass, e.getMessage());
			}
		}
		switch (type) {
			case "py":
			case "jython":
			case TYPE_PYTHON: return new TapPythonEngine(scriptOptions);
			case "js":
			case TYPE_JAVASCRIPT: return new TapRunScriptEngine(scriptOptions);
		}
		return null;
    }
}

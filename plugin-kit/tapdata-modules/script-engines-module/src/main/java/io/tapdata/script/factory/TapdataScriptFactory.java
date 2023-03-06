package io.tapdata.script.factory;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.error.TapAPIErrorCodes;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.script.factory.script.TapRunScriptEngine;

import javax.script.ScriptEngine;

/**
 * @author aplomb
 */
@Implementation(value = ScriptFactory.class, type = "tapdata")
public class TapdataScriptFactory implements ScriptFactory {
    public static void main(String[] args) {
        ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class, "tapdata");
        ScriptEngine scriptEngine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions());
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
						return engineClass.getConstructor(ScriptOptions.class).newInstance(scriptOptions);
					} catch (Throwable e) {
						throw new CoreException(TapAPIErrorCodes.ERROR_INSTANTIATE_ENGINE_CLASS_FAILED, e, "Instantiate engine class {} failed, {}", engineClass, e.getMessage());
					}
				}
				return new TapRunScriptEngine(scriptOptions);
		}
		return null;
    }
}

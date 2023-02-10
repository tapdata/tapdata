package io.tapdata.script.factory;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.utils.InstanceFactory;

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
		return null;
	}
}

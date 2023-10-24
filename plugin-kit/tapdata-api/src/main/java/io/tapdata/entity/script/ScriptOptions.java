package io.tapdata.entity.script;

import io.tapdata.entity.logger.Log;

import javax.script.ScriptEngine;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class ScriptOptions {
	private volatile Map<String, Class<? extends ScriptEngine>> engineCustomClassMap;
	private boolean includeExternalFunctions = true;
	private Log log;
	private ClassLoader classLoader;

	public ScriptOptions classLoader(ClassLoader classLoader){
		this.classLoader = classLoader;
		return this;
	}

	public ScriptOptions log(Log log) {
		this.log = log;
		return this;
	}

	private String engineName;

	public ScriptOptions customEngine(String engineName, Class<? extends ScriptEngine> engineClass) {
		if(engineCustomClassMap == null) {
			synchronized (this) {
				if(engineCustomClassMap == null) {
					engineCustomClassMap = new ConcurrentHashMap<>();
				}
			}
		}
		engineCustomClassMap.put(engineName, engineClass);
		return this;
	}


	public ScriptOptions includeExternalFunctions(boolean includeExternalFunctions) {
		this.includeExternalFunctions = includeExternalFunctions;
	 	return this;
	}

	public boolean isIncludeExternalFunctions() {
		return includeExternalFunctions;
	}

	public ScriptOptions engineName(String engineName) {
		this.engineName = engineName;
		return this;
	}

	public String getEngineName() {
		return engineName;
	}

	public Class<? extends ScriptEngine> getEngineCustomClass(String engineName) {
		return engineCustomClassMap != null ? engineCustomClassMap.get(engineName) : null;
	}

	public ClassLoader getClassLoader() {
		return classLoader;
	}

	public void setClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	public Log getLog() {
		return log;
	}

}

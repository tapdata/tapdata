package io.tapdata.entity.script;

public class ScriptOptions {

	private boolean includeExternalFunctions = true;
	public ScriptOptions includeExternalFunctions(boolean includeExternalFunctions) {
		this.includeExternalFunctions = includeExternalFunctions;
	 	return this;
	}

	private String engineName = "graal.js";
	public ScriptOptions engineName(String engineName) {
		this.engineName = engineName;
		return this;
	}

	public boolean isIncludeExternalFunctions() {
		return includeExternalFunctions;
	}

	public String getEngineName() {
		return engineName;
	}
}

package io.tapdata.entity.script;

public class ScriptOptions {

	private boolean includeExternalFunctions = true;
	private String engineName;

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
}

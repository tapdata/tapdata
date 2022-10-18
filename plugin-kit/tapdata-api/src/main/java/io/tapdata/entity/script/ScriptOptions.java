package io.tapdata.entity.script;

public class ScriptOptions {
	private boolean includeExternalFunctions = true;
	public ScriptOptions includeExternalFunctions(boolean includeExternalFunctions) {
		this.includeExternalFunctions = includeExternalFunctions;
	 	return this;
	}

	public boolean isIncludeExternalFunctions() {
		return includeExternalFunctions;
	}
}

package com.tapdata.processor.dataflow.aggregation;

import com.tapdata.processor.ScriptUtil;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.Map;

public class FilterEval {
	private static final String JS_FILTER_FUNCTION = "filter";

	private static final String FILTER_SCRIPT_FUNCTION = "" +
			"function filter(record){\n" +
			"    if(%s){ return true;}\n" +
			"    return false;\n" +
			"}";

	private String filterPredicate;

	private Invocable engine;

	public FilterEval(String filterPredicate, String jsEngineName) throws ScriptException {
		ScriptEngine scriptEngine = ScriptUtil.getScriptEngine(jsEngineName);

		String buildInMethod = ScriptUtil.initBuildInMethod(null, null);
		String scripts = new StringBuilder(String.format(FILTER_SCRIPT_FUNCTION, filterPredicate)).append(System.lineSeparator()).append(buildInMethod).toString();

		scriptEngine.eval(scripts);
//		Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());
		this.engine = (Invocable) scriptEngine;
		this.filterPredicate = filterPredicate;
	}

	public boolean filter(Map<String, Object> record) {
		Object o = null;
		try {
			o = engine.invokeFunction(JS_FILTER_FUNCTION, record);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		if (o instanceof Boolean) {
			return (boolean) o;
		}
		return false;
	}
}

package io.tapdata.flow.engine.V2.script;

import com.tapdata.constant.BeanUtil;
import com.tapdata.entity.JavaScriptFunctions;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.pdk.apis.error.NotSupportedException;

import javax.script.*;
import java.io.Reader;
import java.util.List;

public class JaradScriptEngine implements ScriptEngine {
	private ScriptEngine scriptEngine;

	private ClientMongoOperator clientMongoOperator;
	public JaradScriptEngine(ScriptEngine scriptEngine) {
		clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
		//TODO get javaScriptFunctions
//		List<JavaScriptFunctions> javaScriptFunctions;
		this.scriptEngine = scriptEngine;
	}
	@Override
	public Object eval(String script, ScriptContext context) throws ScriptException {
		//scriptfunction
		//combine script function.

		return this.scriptEngine.eval(combineFunctions(script), context);
	}

	private String combineFunctions(String script) {
		return null;
	}

	@Override
	public Object eval(Reader reader, ScriptContext context) throws ScriptException {
		throw new NotSupportedException();
//		return scriptEngine.eval(reader, context);
	}

	@Override
	public Object eval(String script) throws ScriptException {
		return scriptEngine.eval(combineFunctions(script));
	}

	@Override
	public Object eval(Reader reader) throws ScriptException {
		throw new NotSupportedException();
//		return scriptEngine.eval(reader);
	}

	@Override
	public Object eval(String script, Bindings n) throws ScriptException {
		return scriptEngine.eval(combineFunctions(script), n);
	}

	@Override
	public Object eval(Reader reader, Bindings n) throws ScriptException {
		throw new NotSupportedException();
//		return scriptEngine.eval(reader, n);
	}

	@Override
	public void put(String key, Object value) {
		scriptEngine.put(key, value);
	}

	@Override
	public Object get(String key) {
		return scriptEngine.get(key);
	}

	@Override
	public Bindings getBindings(int scope) {
		return scriptEngine.getBindings(scope);
	}

	@Override
	public void setBindings(Bindings bindings, int scope) {
		scriptEngine.setBindings(bindings, scope);
	}

	@Override
	public Bindings createBindings() {
		return scriptEngine.createBindings();
	}

	@Override
	public ScriptContext getContext() {
		return scriptEngine.getContext();
	}

	@Override
	public void setContext(ScriptContext context) {
		scriptEngine.setContext(context);
	}

	@Override
	public ScriptEngineFactory getFactory() {
		return scriptEngine.getFactory();
	}
}

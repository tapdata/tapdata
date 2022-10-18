package io.tapdata.flow.engine.V2.script;

import com.oracle.truffle.js.scriptengine.GraalJSScriptEngine;
import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.JobUtil;
import com.tapdata.entity.JavaScriptFunctions;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.processor.ScriptUtil;
import com.tapdata.processor.constant.JSEngineEnum;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.pdk.apis.error.NotSupportedException;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.Value;

import javax.script.*;
import java.io.Reader;
import java.util.List;

public class TapJavaScriptEngine implements ScriptEngine {

    private final ScriptEngine scriptEngine;
    private final String buildInScript;

    public TapJavaScriptEngine(ScriptOptions scriptOptions) {
        ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
        List<JavaScriptFunctions> javaScriptFunctions = JobUtil.getJavaScriptFunctions(clientMongoOperator);
        this.buildInScript = ScriptUtil.initBuildInMethod(javaScriptFunctions, clientMongoOperator);
        this.scriptEngine = initScriptEngine(scriptOptions.getEngineName());
    }

    private ScriptEngine initScriptEngine(String jsEngineName) {
        JSEngineEnum jsEngineEnum = JSEngineEnum.getByEngineName(jsEngineName);
        ScriptEngine scriptEngine;
        if (jsEngineEnum == JSEngineEnum.GRAALVM_JS) {
            scriptEngine = GraalJSScriptEngine
                    .create(null,
                            Context.newBuilder("js")
                                    .allowAllAccess(true)
                                    .allowHostAccess(HostAccess.newBuilder(HostAccess.ALL)
                                            .targetTypeMapping(Value.class, Object.class
                                                    , v -> v.hasArrayElements() && v.hasMembers()
                                                    , v -> v.as(List.class)
                                            ).build()
                                    )
                    );
        } else {
            scriptEngine = new ScriptEngineManager().getEngineByName(jsEngineEnum.getEngineName());
        }
        return scriptEngine;
    }

    @Override
    public Object eval(String script, ScriptContext context) throws ScriptException {
        return this.scriptEngine.eval(combineFunctions(script), context);
    }

    private String combineFunctions(String script) {
        return buildInScript + script;
    }

    @Override
    public Object eval(Reader reader, ScriptContext context) {
        throw new NotSupportedException();
    }

    @Override
    public Object eval(String script) throws ScriptException {
        return scriptEngine.eval(combineFunctions(script));
    }

    @Override
    public Object eval(Reader reader) {
        throw new NotSupportedException();
    }

    @Override
    public Object eval(String script, Bindings n) throws ScriptException {
        return scriptEngine.eval(combineFunctions(script), n);
    }

    @Override
    public Object eval(Reader reader, Bindings n) {
        throw new NotSupportedException();
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

package io.tapdata.flow.engine.V2.script;

import com.oracle.truffle.js.scriptengine.GraalJSScriptEngine;
import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.constant.JobUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.entity.JavaScriptFunctions;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.processor.LoggingOutputStream;
import com.tapdata.processor.ScriptUtil;
import com.tapdata.processor.constant.JSEngineEnum;
import io.tapdata.Application;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.pdk.apis.exception.NotSupportedException;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.logging.log4j.Level;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.Value;

import javax.script.*;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

public class TapJavaScriptEngine implements ScriptEngine, Invocable, Closeable {

    private final ScriptEngine scriptEngine;
    private final Invocable invocable;
    private final String buildInScript;
    private final ScriptExecutorsManager scriptExecutorsManager;

    private URLClassLoader externalJarClassLoader;
    public TapJavaScriptEngine(ScriptOptions scriptOptions) {
        ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
        List<JavaScriptFunctions> javaScriptFunctions = JobUtil.getJavaScriptFunctions(clientMongoOperator);
        this.buildInScript = ScriptUtil.initBuildInMethod(javaScriptFunctions, clientMongoOperator, urlClassLoader -> externalJarClassLoader = urlClassLoader);
        String contextTaskId = Log4jUtil.getContextTaskId();
        Log log = scriptOptions.getLog();
        if (log == null) {
            log = new TapScriptLogger(contextTaskId);
        }
        this.scriptEngine = initScriptEngine(scriptOptions.getEngineName(), log);
        invocable = (Invocable) scriptEngine;
        this.scriptExecutorsManager = new ScriptExecutorsManager(log, clientMongoOperator, HazelcastUtil.getInstance(), contextTaskId, "");
        scriptEngine.put("ScriptExecutorsManager", scriptExecutorsManager);
    }

    private ScriptEngine initScriptEngine(String jsEngineName, Log log) {
        JSEngineEnum jsEngineEnum = JSEngineEnum.getByEngineName(jsEngineName);
        ScriptEngine scriptEngine;
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        LoggingOutputStream out = new LoggingOutputStream(log, Level.INFO);
        LoggingOutputStream err = new LoggingOutputStream(log, Level.ERROR);
        try {
            //need to change as engine classLoader
            Thread.currentThread().setContextClassLoader(Application.class.getClassLoader());
            if (jsEngineEnum == JSEngineEnum.GRAALVM_JS) {
                scriptEngine = GraalJSScriptEngine
                        .create(Engine.newBuilder()
                                        .allowExperimentalOptions(true)
                                        .option("engine.WarnInterpreterOnly", "false")
                                        .out(out)
                                        .err(err)
                                        .build(),
                                Context.newBuilder("js")
                                        .allowAllAccess(true)
                                        .out(out)
                                        .err(err)
                                        .allowHostAccess(HostAccess.newBuilder(HostAccess.ALL)
                                                .targetTypeMapping(Value.class, Object.class
                                                        , v -> v.hasArrayElements() && v.hasMembers()
                                                        , v -> v.as(List.class)
                                                ).build()
                                        )
                        );
                SimpleScriptContext scriptContext = new SimpleScriptContext();
                scriptContext.setWriter(new OutputStreamWriter(out));
                scriptContext.setErrorWriter(new OutputStreamWriter(err));
                scriptEngine.setContext(scriptContext);
            } else {
                scriptEngine = new ScriptEngineManager().getEngineByName(jsEngineEnum.getEngineName());
            }
        } finally {
            //return pdk classLoader
            Thread.currentThread().setContextClassLoader(classLoader);
        }
        scriptEngine.put("log", log);
        return scriptEngine;
    }

    @Override
    public Object eval(String script, ScriptContext context) {
        return applyClassLoaderContext(() -> scriptEngine.eval(combineFunctions(script), context));
    }

    //merge customize functions
    private String combineFunctions(String script) {
        return buildInScript + "\n" + script;
    }

    public Object applyClassLoaderContext(Callable<?> callable) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        ClassLoader theClassLoader = externalJarClassLoader;
        if(theClassLoader == null)
            theClassLoader = Application.class.getClassLoader();
        Thread.currentThread().setContextClassLoader(theClassLoader);
        try {
            return callable.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            Thread.currentThread().setContextClassLoader(classLoader);
        }
    }

    @Override
    public Object eval(Reader reader, ScriptContext context) {
        throw new NotSupportedException();
    }

    @Override
    public Object eval(String script) {
        return applyClassLoaderContext(() -> scriptEngine.eval(combineFunctions(script)));
    }

    @Override
    public Object eval(Reader reader) {
        throw new NotSupportedException();
    }

    @Override
    public Object eval(String script, Bindings n) {
        return applyClassLoaderContext(() -> scriptEngine.eval(combineFunctions(script), n));
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

    @Override
    public Object invokeMethod(Object thiz, String name, Object... args) throws ScriptException, NoSuchMethodException {
        return invocable.invokeMethod(thiz, name, args);
    }

    @Override
    public Object invokeFunction(String name, Object... args) throws ScriptException, NoSuchMethodException {
        return invocable.invokeFunction(name, args);
    }

    @Override
    public <T> T getInterface(Class<T> clasz) {
        return invocable.getInterface(clasz);
    }

    @Override
    public <T> T getInterface(Object thiz, Class<T> clasz) {
        return invocable.getInterface(thiz, clasz);
    }

    @Override
    public void close() throws IOException {
        String tag = this.getClass().getSimpleName();
        CommonUtils.ignoreAnyError(() -> Optional.ofNullable(this.scriptExecutorsManager).ifPresent(ScriptExecutorsManager::close), tag);
        CommonUtils.ignoreAnyError(() -> {
            if (this.scriptEngine instanceof GraalJSScriptEngine) {
                ((GraalJSScriptEngine) this.scriptEngine).close();
            }
        }, tag);
    }
}

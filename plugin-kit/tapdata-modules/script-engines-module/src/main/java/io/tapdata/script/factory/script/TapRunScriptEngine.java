package io.tapdata.script.factory.script;

import com.oracle.truffle.js.scriptengine.GraalJSScriptEngine;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.pdk.apis.exception.NotSupportedException;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.Value;

import javax.script.Bindings;
import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;
import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

public class TapRunScriptEngine implements ScriptEngine, Invocable, Closeable {
    private ScriptEngine scriptEngine;
    private final Invocable invocable;
    private final String buildInScript;
    private ClassLoader classLoader;

    public Invocable invocable() {
        return this.invocable;
    }

    public TapRunScriptEngine(ScriptOptions scriptOptions) {
        classLoader = scriptOptions.getClassLoader();
        this.buildInScript = "";
        this.scriptEngine = initScriptEngine(scriptOptions.getEngineName());
        this.invocable = (Invocable) this.scriptEngine;
    }
    private ScriptEngine initScriptEngine(String jsEngineName) {
        EngineType jsEngineEnum = EngineType.getByEngineName(jsEngineName);
        ScriptEngine scriptEngine;
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(Optional.ofNullable(this.classLoader).orElse(Thread.currentThread().getContextClassLoader()));
            if (jsEngineEnum == EngineType.GRAALVM_JS) {
                scriptEngine = GraalJSScriptEngine
                        .create(Engine.newBuilder()
                                        .allowExperimentalOptions(true)
                                        .option("engine.WarnInterpreterOnly", "false")
                                        .build(),
                                Context.newBuilder("js")
                                        .allowAllAccess(true)
                                        .allowHostAccess(HostAccess.newBuilder(HostAccess.ALL)
                                                .targetTypeMapping(Value.class, Object.class
                                                        , v -> v.hasArrayElements() && v.hasMembers()
                                                        , v -> v.as(List.class)
                                                ).build()
                                        )
                        );
                SimpleScriptContext scriptContext = new SimpleScriptContext();
                scriptEngine.setContext(scriptContext);
            } else {
                scriptEngine = new ScriptEngineManager().getEngineByName(jsEngineEnum.engineName());
            }
        } catch (Exception e) {
            scriptEngine = new ScriptEngineManager().getEngineByName(jsEngineEnum.engineName());
        } finally {
            Thread.currentThread().setContextClassLoader(classLoader);
        }
        return scriptEngine;
    }

    public Object applyClassLoaderContext(Callable<?> callable) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(Optional.ofNullable(this.classLoader).orElse(Thread.currentThread().getContextClassLoader()));
        try {
            return callable.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            Thread.currentThread().setContextClassLoader(classLoader);
        }
    }

    private String combineFunctions(String script) {
        return buildInScript + "\n" + script;
    }

    @Override
    public Object eval(String script, ScriptContext context) throws ScriptException {
        return applyClassLoaderContext(() -> this.scriptEngine.eval(combineFunctions(script), context));
    }

    @Override
    public Object eval(Reader reader, ScriptContext context) throws ScriptException {
        throw new NotSupportedException();
    }

    @Override
    public Object eval(String script) throws ScriptException {
        return applyClassLoaderContext(() -> this.scriptEngine.eval(combineFunctions(script)));
    }

    @Override
    public Object eval(Reader reader) throws ScriptException {
        try {
            return applyClassLoaderContext(() -> this.scriptEngine.eval(reader));
        }finally {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public Object eval(String script, Bindings n) throws ScriptException {
        return applyClassLoaderContext(() -> this.scriptEngine.eval(combineFunctions(script), n));
    }

    @Override
    public Object eval(Reader reader, Bindings n) throws ScriptException {
        throw new NotSupportedException();
    }

    @Override
    public void put(String key, Object value) {
        this.scriptEngine.put(key, value);
    }

    @Override
    public Object get(String key) {
        return this.scriptEngine.get(key);
    }

    @Override
    public Bindings getBindings(int scope) {
        return this.scriptEngine.getBindings(scope);
    }

    @Override
    public void setBindings(Bindings bindings, int scope) {
        this.scriptEngine.setBindings(bindings, scope);
    }

    @Override
    public Bindings createBindings() {
        return this.scriptEngine.createBindings();
    }

    @Override
    public ScriptContext getContext() {
        return this.scriptEngine.getContext();
    }

    @Override
    public void setContext(ScriptContext context) {
        this.scriptEngine.setContext(context);
    }

    @Override
    public ScriptEngineFactory getFactory() {
        return this.scriptEngine.getFactory();
    }

    @Override
    public void close() throws IOException {
        String tag = this.getClass().getSimpleName();
        CommonUtils.ignoreAnyError(() -> {
            if (this.scriptEngine instanceof GraalJSScriptEngine) {
                ((GraalJSScriptEngine) this.scriptEngine).close();
            }
        }, tag);
    }

    @Override
    public Object invokeMethod(Object thiz, String name, Object... args) throws ScriptException, NoSuchMethodException {
        return this.invocable.invokeMethod(thiz, name, args);
    }

    @Override
    public Object invokeFunction(String name, Object... args) throws ScriptException, NoSuchMethodException {
        return this.invocable.invokeFunction(name, args);
    }

    @Override
    public <T> T getInterface(Class<T> clasz) {
        return this.invocable.getInterface(clasz);
    }

    @Override
    public <T> T getInterface(Object thiz, Class<T> clasz) {
        return this.invocable.getInterface(thiz, clasz);
    }

    public static enum EngineType {
        NASHORN("nashorn"),
        GRAALVM_JS("graal.js");

        String name;

        EngineType(String name) {
            this.name = name;
        }

        public static EngineType getByEngineName(String name) {
            if (null != name && !"".equals(name.trim())) {
                EngineType[] values = values();
                for (EngineType value : values) {
                    if (value.name.equals(name)) return value;
                }
            }
            return GRAALVM_JS;
        }

        public String engineName() {
            return this.name;
        }
    }
}

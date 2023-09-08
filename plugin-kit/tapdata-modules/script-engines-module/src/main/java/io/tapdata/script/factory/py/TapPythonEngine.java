package io.tapdata.script.factory.py;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.pdk.apis.exception.NotSupportedException;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.python.core.PyException;
import org.python.jsr223.PyScriptEngineFactory;

import javax.script.Bindings;
import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * @author GavinXiao
 * @description TapPythonEngine create by Gavin
 * @create 2023/6/13 15:58
 **/
public class TapPythonEngine implements ScriptEngine, Invocable, Closeable {
    private final ScriptEngine scriptEngine;
    private final Invocable invocable;
    private final String buildInScript;
    private final ClassLoader classLoader;
    private final Log logger;

    public Invocable invocable() {
        return this.invocable;
    }

    public TapPythonEngine(ScriptOptions scriptOptions) {
        this.logger = Optional.ofNullable(scriptOptions.getLog()).orElse(new TapLog());
        if (!new File(PythonUtils.getThreadPackagePath()).exists()) {
            PythonUtils.execute(PythonUtils.PYTHON_THREAD_JAR, PythonUtils.PYTHON_THREAD_PACKAGE_PATH, logger);
        }
        classLoader = scriptOptions.getClassLoader();
        this.buildInScript = "";
        this.scriptEngine = initScriptEngine(scriptOptions.getEngineName());
        this.invocable = (Invocable) this.scriptEngine;
    }
    private ScriptEngine initScriptEngine(String engineName) {
        TapPythonEngine.EngineType jsEngineEnum = TapPythonEngine.EngineType.getByEngineName(engineName);
        ScriptEngine scriptEngine;
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(Optional.ofNullable(this.classLoader).orElse(Thread.currentThread().getContextClassLoader()));
            if (EngineType.Python.engineName().equals(engineName)) {
                //new org.python.jsr223.PyScriptEngine();
                try {
                    scriptEngine = new PyScriptEngineFactory().getScriptEngine();
                }catch (PyException e){
                    scriptEngine = new PyScriptEngineFactory().getScriptEngine();
                }
                SimpleScriptContext scriptContext = new SimpleScriptContext();
                scriptEngine.setContext(scriptContext);
            } else {
                scriptEngine = new ScriptEngineManager().getEngineByName(jsEngineEnum.engineName());
            }
        } catch (Exception e) {
            //TapLogger.debug("Python Eninge", "Can not init python engine, goto init default.");
            //scriptEngine = new ScriptEngineManager().getEngineByName(jsEngineEnum.engineName());
            throw new CoreException("Can not init python engine, error msg: " + e.getMessage());
        } finally {
            Thread.currentThread().setContextClassLoader(classLoader);
        }
        Optional.ofNullable(scriptEngine).ifPresent(s -> {
            try{
                s.eval(String.format("import sys\nsys.path.append('%s');", PythonUtils.getThreadPackagePath()));
            } catch (Exception error){
                logger.warn("Unable to load Python's third-party dependencies from the third-party dependencies package directory: {}, msg: {}", PythonUtils.PYTHON_THREAD_PACKAGE_PATH, error.getMessage());
            }
        });
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
                logger.warn(e.getMessage());
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
            if (this.scriptEngine instanceof org.python.jsr223.PyScriptEngine) {
                ((org.python.jsr223.PyScriptEngine) this.scriptEngine).close();
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
        Python("python")
        ;

        String name;

        EngineType(String name) {
            this.name = name;
        }

        public static TapPythonEngine.EngineType getByEngineName(String name) {
            if (null != name && !"".equals(name.trim())) {
                TapPythonEngine.EngineType[] values = values();
                for (TapPythonEngine.EngineType value : values) {
                    if (value.name.equals(name)) return value;
                }
            }
            return Python;
        }

        public String engineName() {
            return this.name;
        }
    }
}

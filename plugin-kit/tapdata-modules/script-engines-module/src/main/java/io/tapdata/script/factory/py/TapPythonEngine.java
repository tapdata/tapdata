package io.tapdata.script.factory.py;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.pdk.apis.exception.NotSupportedException;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.python.core.PrePy;
import org.python.core.Py;
import org.python.core.PyList;
import org.python.core.PySystemState;
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
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * @author GavinXiao
 * @description TapPythonEngine create by Gavin
 * @create 2023/6/13 15:58
 **/
public class TapPythonEngine implements ScriptEngine, Invocable, Closeable {
    public static final int ERROR_PY_NODE_CODE = 1000010;
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
        File file = PythonUtils.getThreadPackagePath();
        if (null == file) {
            PythonUtils.flow(logger);
            try {
                String jarPath = "py-lib/jython-standalone-2.7.3.jar";
                ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                URL jarURL = new URL("file://" + jarPath);
                Method addURLMethod = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
                addURLMethod.setAccessible(true);
                addURLMethod.invoke(classLoader, jarURL);
            } catch (Exception e) {}
        }
        classLoader = scriptOptions.getClassLoader();
        this.buildInScript = "";
        this.scriptEngine = initScriptEngine(scriptOptions.getEngineName());
        this.invocable = (Invocable) this.scriptEngine;
    }
    private ScriptEngine initScriptEngine(String engineName) {
        TapPythonEngine.EngineType engineEnum = TapPythonEngine.EngineType.getByEngineName(engineName);
        ScriptEngine scriptEngine = null;
        //ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try {
            //Thread.currentThread().setContextClassLoader(Optional.ofNullable(this.classLoader).orElse(Thread.currentThread().getContextClassLoader()));
            try {
                Field field = PySystemState.class.getDeclaredField("initialized");
                field.setAccessible(true);
                Field defaultArgvF = PySystemState.class.getDeclaredField("defaultArgv");
                defaultArgvF.setAccessible(true);
                Field defaultPathF = PySystemState.class.getDeclaredField("defaultPath");
                defaultPathF.setAccessible(true);
                Boolean initialized = (Boolean) field.get(null);
                field.set(null, false);

                try {
                    String jarFileName = Py.getJarFileName();
                    logger.info("JarFileName: {}", jarFileName);
                    if (!new File(jarFileName).exists()) {
                        String replace = jarFileName.replace("jython-standalone-2.7.3.jar", "");
                        File file = new File(replace);
                        if (!file.exists()) {
                            file.mkdirs();
                            PythonUtils.copyFile(new File("py-lib/jython-standalone-2.7.3.jar"), file);
                        }
                    }
                } catch (Exception e) {}

                logger.info("SystemProperties: {}", PrePy.getSystemProperties());

                PyList defaultArgv = (PyList)(defaultArgvF.get(null)) ;
                PyList defaultPath = (PyList)(defaultPathF.get(null)) ;
                logger.info("before initialized: {}, defaultArgv: {}, defaultPath: {}, SystemProperties: {}", initialized, defaultArgv, defaultPath, PySystemState.registry);
                if (null == defaultArgv ) {
                    defaultArgvF.set(null, new PyList());
                }
                if (null == defaultPath) {
                    defaultPathF.set(null, new PyList());
                }

                PySystemState.initialize(null, null, new String[]{""}, Thread.currentThread().getContextClassLoader());
                logger.info("after initialized: {}, defaultArgv: {}, defaultPath: {}, SystemProperties: {}", initialized, defaultArgv, defaultPath, PySystemState.registry);
                if (null == PySystemState.registry){
                    PySystemState.registry = PrePy.getSystemProperties();
                }


                scriptEngine = new ScriptEngineManager().getEngineByName(engineEnum.name);
                if (null == scriptEngine) {
                    scriptEngine = new PyScriptEngineFactory().getScriptEngine();
                }
            } catch (Exception e) {
                scriptEngine = new PyScriptEngineFactory().getScriptEngine();
            }
            if (Objects.nonNull(scriptEngine)) {
                SimpleScriptContext scriptContext = new SimpleScriptContext();
                scriptEngine.setContext(scriptContext);
                try{
                    File file = PythonUtils.getThreadPackagePath();
                    if (null != file) {
                        logger.info(String.format("import sys\\nsys.path.append('%s')", file.getAbsolutePath()));
                        scriptEngine.eval(String.format("\nimport sys\nsys.path.append('%s')\n", file.getAbsolutePath()));
                        PythonUtils.supportThirdPartyPackageList(file, logger);
                    } else {
                        logger.warn("Unable to load Python's third-party dependencies from the third-party dependencies package directory");
                    }
                } catch (Exception error){
                    logger.warn("Unable to load Python's third-party dependencies from the third-party dependencies package directory: {}, msg: {}", PythonUtils.PYTHON_THREAD_PACKAGE_PATH, error.getMessage());
                }
            }
        } catch (Exception e) {
            //logger.error("Can not init python engine, error msg: {}", e.getMessage());
            throw new CoreException(ERROR_PY_NODE_CODE, e, "Can not init python engine, error msg: {}", e.getMessage());
        } finally {
            //Thread.currentThread().setContextClassLoader(classLoader);
        }
        return scriptEngine;
    }

    public Object applyClassLoaderContext(Callable<?> callable) {
        //ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        //Thread.currentThread().setContextClassLoader(Optional.ofNullable(this.classLoader).orElse(Thread.currentThread().getContextClassLoader()));
        try {
            return callable.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
           // Thread.currentThread().setContextClassLoader(classLoader);
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
        } finally {
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
        Python("jython")
        ;

        String name;

        EngineType(String name) {
            this.name = name;
        }

        public static TapPythonEngine.EngineType getByEngineName(String name) {
//            if (null != name && !"".equals(name.trim())) {
//                TapPythonEngine.EngineType[] values = values();
//                for (TapPythonEngine.EngineType value : values) {
//                    if (value.name.equals(name)) return value;
//                }
//            }
            return Python;
        }

        public String engineName() {
            return this.name;
        }
    }
}

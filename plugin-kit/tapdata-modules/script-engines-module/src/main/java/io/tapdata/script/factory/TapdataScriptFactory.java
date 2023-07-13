package io.tapdata.script.factory;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.error.TapAPIErrorCodes;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.script.factory.py.TapPythonEngine;
import io.tapdata.script.factory.script.TapRunScriptEngine;

import javax.script.ScriptEngine;

/**
 * @author aplomb
 */
@Implementation(value = ScriptFactory.class, type = "tapdata")
public class TapdataScriptFactory implements ScriptFactory {
    public static void main(String[] args) {
        ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class, "tapdata");
        ScriptEngine javaScriptEngine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions());
        ScriptEngine pythonEngine = scriptFactory.create(ScriptFactory.TYPE_PYTHON, new ScriptOptions());
		try {
			pythonEngine.eval("import java.util.ArrayList as ArrayList");
		}catch (Exception e){}



//        fileToString();
//		try {
//			pythonEngine.eval("from jnius import autoclass");
//		}catch (Exception e){
//			e.printStackTrace();
//		}
//		String pipPath = "D:\\GavinData\\deskTop\\Lib";
//		try {
//			PythonInterpreter interpreter = new PythonInterpreter();
//			PySystemState sys = Py.getSystemState();
//
//			System.out.println(sys.path);
//
//
//
//			sys.path.add(pipPath);
//			interpreter.exec("import sys");
//			interpreter.exec("print sys.path");
//			interpreter.exec("path = \"" + pipPath + "\"");
//			interpreter.exec("sys.path.append(path)");
//			interpreter.exec("print sys.path");
//			//interpreter.exec("a=3; b=5;");
//
//			//InputStream filepy = new FileInputStream("E:\\input.py");
//			//interpreter.execfile(filepy);
//			//filepy.close();
//		} catch (Exception e){
//			e.printStackTrace();
//		}
		try {
			pythonEngine.eval("import requests;");
		} catch (Exception e) {
			e.printStackTrace();
		}




//		try {
//			Properties props = new Properties();
//			props.put("python.home", "D:\\GavinData\\deskTop\\Lib\\site-packages\\requests-2.2.1-py2.7.egg");
//			props.put("python.console.encoding", "UTF-8");
//			props.put("python.security.respectJavaAccessibility", "false");
//			props.put("python.import.site", "false");
//			Properties preprops = System.getProperties();
//			PythonInterpreter.initialize (preprops, props, new String[0]);
//			PythonInterpreter interpreter = new PythonInterpreter();
//			pythonEngine.eval("import requests;");
//		}catch (Exception e){
//			e.printStackTrace();
//		}
	}

    @Override
    public ScriptEngine create(String type, ScriptOptions scriptOptions) {
		Class<? extends ScriptEngine> engineClass = scriptOptions.getEngineCustomClass(type);
		if(engineClass != null) {
			try {
				return engineClass.getConstructor(ScriptOptions.class).newInstance(scriptOptions);
			} catch (Throwable e) {
				throw new CoreException(TapAPIErrorCodes.ERROR_INSTANTIATE_ENGINE_CLASS_FAILED, e, "Instantiate engine class {} failed, {}", engineClass, e.getMessage());
			}
		}
		switch (type) {
			case TYPE_PYTHON: return new TapPythonEngine(scriptOptions);
			case TYPE_JAVASCRIPT: return new TapRunScriptEngine(scriptOptions);
		}
		return null;
    }
}

package io.tapdata.script.factory;

import com.alibaba.fastjson.JSONArray;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.error.TapAPIErrorCodes;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.pdk.core.error.PDKRunnerErrorCodes;
import io.tapdata.script.factory.py.TapPythonEngine;
import io.tapdata.script.factory.script.TapRunScriptEngine;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.python.core.Py;
import org.python.core.PySystemState;
import org.python.util.PythonInterpreter;

import javax.script.ScriptEngine;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.Objects;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * @author aplomb
 */
@Implementation(value = ScriptFactory.class, type = "tapdata")
public class TapdataScriptFactory implements ScriptFactory {
//	private static final PythonInterpreter intr = new PythonInterpreter();
//
//	public static String fileToString(InputStream connectorJsStream) throws IOException {
//		Reader reader = null;
//		Writer writer = new StringWriter();
//		char[] buffer = new char[1024];
//		try {
//			reader = new BufferedReader(new InputStreamReader(connectorJsStream, StandardCharsets.UTF_8));
//			int n;
//			while ((n = reader.read(buffer)) != -1) {
//				writer.write(buffer, 0, n);
//			}
//		} catch (Exception ignored) {
//		} finally {
//			if (Objects.nonNull(reader)) {
//				reader.close();
//				writer.close();
//				connectorJsStream.close();
//			}
//		}
//		return writer.toString();
//	}
//	public static void fileToString(){
//		intr.exec("import sys");
//
//		try {
//			// 启动子进程，运行本地安装的 Python，获取 sys.path 配置
//			Process p = Runtime.getRuntime().exec(new String[]{
//					"python", "-c", "import json; import sys; print json.dumps(sys.path)"});
//			p.waitFor();
//
//			// 从中获取到相关的 PIP 安装路径，放入 Jython 的 sys.path
//			String stdout = fileToString(p.getInputStream());
//			JSONArray syspathRaw = JSONArray.parseArray(stdout);
//			for (int i = 0; i < syspathRaw.size(); i++) {
//				String path = syspathRaw.getString(i);
//				if (path.contains("site-packages") || path.contains("dist-packages"))
//					intr.exec(String.format("sys.path.insert(0, '%s')", path));
//			}
//		} catch (Exception ex) {}
//	}
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

	//    		try {
	//		if (!Application.class.getResource("").getProtocol().equals("jar")) {
	//			// 以 idea 的方式运行
	//			//ClassLoader defaultClassLoader = Application.class.getClassLoader();
	//			//InputStream inputStreamPy = defaultClassLoader.getResourceAsStream("BOOT-INF/lib/jython-standalone-2.7.2.jar");
	//			//InputStream inputStreamEngine = defaultClassLoader.getResourceAsStream("BOOT-INF/lib/script-engine-module-1.0-SNAPSHOT.jar");
	//		} else {
	//			// 以 jar 的方式运行
	//			String sc = null;
	//			try {
	//				//假设读取META-INF/MANIFEST.MF文件
	//				Enumeration<URL> urls = Application.class.getClassLoader().getResources("META-INF/MANIFEST.MF");
	//				while (urls.hasMoreElements() && null == sc) {
	//					URL url = urls.nextElement();
	//					String path = url.getPath();
	//					if (path.contains("script-engine-module-1.0-SNAPSHOT.jar")) {
	//						sc = path;
	//					}
	//				}
	//			} catch (IOException e) { }
	//			if (sc != null) {
	//				String jarPath = sc;
	//				String scPath = sc.replace("file://", "").replace("/script-engine-module-1.0-SNAPSHOT.jar!/META-INF/MANIFEST.MF", "");
	//				File scFile = new File(scPath);
	//				JarFile jar = new JarFile(jarPath);
	//				Enumeration<?> entries = jar.entries();
	//				while (entries.hasMoreElements()) {
	//					JarEntry entry = (JarEntry) entries.nextElement();
	//					if (entry.getName().endsWith("Lib.jar")) {
	//						try (InputStream inputStream = jar.getInputStream(entry); OutputStream scStream = FileUtils.openOutputStream(scFile)) {
	//							org.apache.commons.io.IOUtils.copyLarge(inputStream, scStream);
	//						} catch (Exception e){
	//							e.printStackTrace();
	//						}
	//						break;
	//					}
	//				}
	//
	//			}
	//		}
	//		MyService bean = BeanUtil.getBean(MyService.class);
	//		bean.readResourceFile("script-engine-module-1.0-SNAPSHOT.jar");
	//	}catch (Exception e){
	//
	//	}

	public static void unzip(File zipFile, File outputDir) {
		if (zipFile == null || outputDir == null)
			throw new CoreException(PDKRunnerErrorCodes.COMMON_ILLEGAL_PARAMETERS, "Unzip missing zipFile or outputPath");
		if (outputDir.isFile())
			throw new CoreException(PDKRunnerErrorCodes.CLI_UNZIP_DIR_IS_FILE, "Unzip director is a file, expect to be directory or none");

		try (ZipFile zf = new ZipFile(zipFile)) {

			if (!outputDir.exists())
				FileUtils.forceMkdir(outputDir);

			Enumeration<? extends ZipEntry> zipEntries = zf.entries();
			while (zipEntries.hasMoreElements()) {
				ZipEntry entry = zipEntries.nextElement();

				try {
					if (entry.isDirectory()) {
						String entryPath = FilenameUtils.concat(outputDir.getAbsolutePath(), entry.getName());
						FileUtils.forceMkdir(new File(entryPath));
					} else {
						String entryPath = FilenameUtils.concat(outputDir.getAbsolutePath(), entry.getName());
						try(OutputStream fos = FileUtils.openOutputStream(new File(entryPath))) {
							org.apache.commons.io.IOUtils.copyLarge(zf.getInputStream(entry), fos);
						}
					}
				} catch (IOException ei) {
					ei.printStackTrace();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

package io.tapdata.script.factory;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.error.TapAPIErrorCodes;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.script.factory.py.TapPythonEngine;
import io.tapdata.pdk.core.error.PDKRunnerErrorCodes;
import io.tapdata.script.factory.script.TapRunScriptEngine;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import javax.script.ScriptEngine;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

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

		try {
			pythonEngine.eval("import requests;");
		} catch (Exception e) {
			e.printStackTrace();
		}
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
	//			//InputStream inputStreamPy = defaultClassLoader.getResourceAsStream("BOOT-INF/lib/jython-standalone-2.7.3.jar");
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

/**
 * @title: JobStatusUtils
 * @description:
 * @author lk
 * @date 2021/7/19
 */
package com.tapdata.job;

import java.io.File;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

@Slf4j
public class JobStatusUtils {

	private static final Map<String, JobStatusProcessor> jobStatusProcessorHashMap = new HashMap<>();

	public static final String CLASS_SUFFIX = ".class";

	static {
		try {
			buildStatusProcessor();
		} catch (Exception e) {
			log.error("Build processor failed,message: {}", e.getMessage(), e);
		}
	}

	public static JobStatusProcessor getProcessor(String target) {
		JobStatusProcessor jobStatusProcessor = jobStatusProcessorHashMap.get(target);
		if (jobStatusProcessor == null){
			throw new RuntimeException("xxxxxxxxxxxxxxxxx");
		}
		return jobStatusProcessor;
	}

	private static void buildStatusProcessor() throws Exception {
		Set<Class<?>> set = new HashSet<>();
		Enumeration<URL> urls = Thread.currentThread().getContextClassLoader().getResources("com/tapdata/manager/job");
		while (urls != null && urls.hasMoreElements()){
			URL url = urls.nextElement();
			String protocol = url.getProtocol();
			log.info("Urls path: {}, protocol: {}", url.getPath(), url.getProtocol());
			if ("file".equals(protocol)) {
				String file = URLDecoder.decode(url.getFile(), "UTF-8");
				File dir = new File(file);
				if (dir.isDirectory()) {
					parseClassFile(dir, set);
				}
			}else if ("jar".equals(protocol)) {
				JarFile jar = ((JarURLConnection) url.openConnection()).getJarFile();
				Enumeration<JarEntry> entries = jar.entries();
				while (entries.hasMoreElements()) {
					JarEntry entry = entries.nextElement();
					if (entry.isDirectory()) {
						continue;
					}
					String name = entry.getName();
					if (name.endsWith(CLASS_SUFFIX)) {
						set.add(Class.forName(name.substring(0, name.length() - CLASS_SUFFIX.length()).replaceAll("/", ".")));
					}
				}
			}
		}

		if (CollectionUtils.isNotEmpty(set)){
			JobStatusProcessor defaultProcessor = null;
			for (Class<?> aClass : set) {
				if (!aClass.isInterface() && aClass.isAnnotationPresent(JobStatusHandler.class)) {
					JobStatusHandler declaredAnnotation = aClass.getDeclaredAnnotation(JobStatusHandler.class);
					for (JobStatus jobStatus : declaredAnnotation.value()) {
						Object o = aClass.getDeclaredConstructor().newInstance();
						if (o instanceof JobStatusProcessor){
							jobStatusProcessorHashMap.put(jobStatus.getTarget(), (JobStatusProcessor) o);
						}
					}
				}
				if (aClass == JobStatusDefaultProcessor.class){
					defaultProcessor = (JobStatusProcessor) aClass.getDeclaredConstructor().newInstance();
				}
			}
			for (String targetstatus : JobStatus.getTargetstatus()) {
				if (!jobStatusProcessorHashMap.containsKey(targetstatus)){
					jobStatusProcessorHashMap.put(targetstatus, defaultProcessor);
				}
			}
		}else {
			log.warn("GetProcessor warn, set is empty");
		}
	}



	private static void parseClassFile(File dir, Set<Class<?>> set) throws ClassNotFoundException {
		log.info("Parse class file start, dir.name: {}", dir.getName());
		if (dir.isDirectory()) {
			File[] files = dir.listFiles();
			if (files == null){
				log.warn("parse file failed, files is null");
				return;
			}
			for (File file : files) {
				parseClassFile(file, set);
			}
		} else if (dir.getName().endsWith(CLASS_SUFFIX)) {
			String name = dir.getPath();
			name = name.substring(name.indexOf("classes") + 8).replace("\\", ".");

			set.add(Class.forName(name.substring(0, name.length() - CLASS_SUFFIX.length()).replaceAll("/", ".")));
		}
	}
}

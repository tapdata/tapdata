package com.tapdata.constant;

import com.tapdata.entity.DatabaseTypeEnum;
import io.tapdata.annotation.DatabaseTypeAnnotation;
import io.tapdata.annotation.DatabaseTypeAnnotations;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Pattern;

/**
 * @author samuel
 * @Description
 * @create 2020-09-02 10:22
 **/
public class BeanUtil {

	private static Logger logger = LogManager.getLogger(BeanUtil.class);
	public static ConfigurableApplicationContext configurableApplicationContext;
	private static final String CLASS_SUFFIX = ".class";
	private static final Pattern INNER_PATTERN = Pattern.compile("\\$(\\d+).", Pattern.CASE_INSENSITIVE);
	public static final String TAPDATA_DEFAULT_PACKAGE_PATH = "io.tapdata";

	public static <T> T getBean(Class<T> clazz) {
		if (clazz == null || configurableApplicationContext == null) {
			return null;
		}

		return configurableApplicationContext.getBean(clazz);
	}

	public static Set<Class<?>> findMatchComponents(String packageName) throws IOException {
		if (packageName.endsWith(".")) {
			packageName = packageName.substring(0, packageName.length() - 1);
		}
		Map<String, String> classMap = new HashMap<>();
		String path = packageName.replace(".", "/");

		logger.debug("Start find all url path from package[" + packageName + "]......");
		Enumeration<URL> urls = findAllClassPathResources(path);

		int count = 0;

		while (urls != null && urls.hasMoreElements()) {
			count++;
			URL url = urls.nextElement();
			String protocol = url.getProtocol();
			if ("file".equals(protocol)) {
				String file = URLDecoder.decode(url.getFile(), "UTF-8");
				File dir = new File(file);
				if (dir.isDirectory()) {
					parseClassFile(dir, packageName, classMap);
				} else {
					throw new IllegalArgumentException("file must be directory");
				}
			} else if ("jar".equals(protocol)) {
				parseJarFile(url, classMap);
			}
		}

		logger.debug("Found " + count + " urls,classMap size = " + classMap.size());

		Set<Class<?>> set = new HashSet<>(classMap.size());
		for (String key : classMap.keySet()) {
			String className = classMap.get(key);
			try {
				set.add(Class.forName(className.replaceAll("/", ".")));
			} catch (ClassNotFoundException e) {
				logger.error("Class[" + className + "] not found Exception");
			}
		}
		return set;
	}

	protected static Enumeration<URL> findAllClassPathResources(String path) throws IOException {
		if (path.startsWith("/")) {
			path = path.substring(1);
		}
		Enumeration<URL> urls = Thread.currentThread().getContextClassLoader().getResources(path);

		return urls;
	}

	protected static void parseJarFile(URL url, Map<String, String> classMap) throws IOException {
		JarFile jar = ((JarURLConnection) url.openConnection()).getJarFile();
		Enumeration<JarEntry> entries = jar.entries();
		while (entries.hasMoreElements()) {
			JarEntry entry = entries.nextElement();
			if (entry.isDirectory()) {
				continue;
			}
			String name = entry.getName();
			if (name.endsWith(CLASS_SUFFIX)) {
				addToClassMap(name.replace("/", "."), classMap);
			}
		}
	}

	protected static void parseClassFile(File dir, String packageName, Map<String, String> classMap) {
		if (dir.isDirectory()) {
			File[] files = dir.listFiles();
			for (File file : files) {
				parseClassFile(file, packageName, classMap);
			}
		} else if (dir.getName().endsWith(CLASS_SUFFIX)) {
			String name = dir.getPath();
			name = name.substring(name.indexOf("classes") + 8).replace("\\", ".");
			addToClassMap(name, classMap);
		}
	}

	private static boolean addToClassMap(String name, Map<String, String> classMap) {

		if (INNER_PATTERN.matcher(name).find()) { //过滤掉匿名内部类
			logger.debug("anonymouns inner class : " + name);
			return false;
		}
		logger.debug("class:" + name);
		if (name.indexOf("$") > 0) { //内部类
			logger.debug("inner class:" + name);
		}
		if (!classMap.containsKey(name)) {
			classMap.put(name, name.substring(0, name.length() - CLASS_SUFFIX.length())); //去掉尾缀
		}
		return true;
	}

	/**
	 * Get the implemented interface class
	 *
	 * @param clazz             Class to get
	 * @param includeSuperClass Whether to include the interface implemented by the parent class
	 * @return Returns the collection of interface classes
	 */
	public static List<Class<?>> getInterfaces(Class<?> clazz, boolean includeSuperClass) {
		if (clazz == null) {
			return null;
		}
		// get clazz's interfaces
		List<Class<?>> interfaces = new ArrayList<>(Arrays.asList(clazz.getInterfaces()));

		if (includeSuperClass) {
			// Recursively get the interface class implemented by the parent class
			getInterfaces(clazz, interfaces);
		}

		return interfaces;
	}

	/**
	 * 获取父类实现的接口类
	 *
	 * @param clazz      Class to get
	 * @param interfaces Collection of interface classes
	 */
	private static void getInterfaces(Class<?> clazz, List<Class<?>> interfaces) {
		Class<?> superclass = clazz.getSuperclass();
		if (superclass == null || superclass.getName().equals(Object.class.getName())) {
			return;
		}

		interfaces.addAll(Arrays.asList(superclass.getInterfaces()));

		// Get interface class recursively
		getInterfaces(superclass, interfaces);
	}

	/**
	 * Determine whether the desired interface class is implemented
	 *
	 * @param clazz             Checked class
	 * @param expectInterface   Expected interface class
	 * @param includeSuperClass Determine whether to include the parent class
	 * @return
	 */
	public static boolean hasInterface(Class<?> clazz, Class<?> expectInterface, boolean includeSuperClass) {
		if (clazz == null || expectInterface == null) {
			return false;
		}

		List<Class<?>> interfaces = getInterfaces(clazz, includeSuperClass);
		if (CollectionUtils.isEmpty(interfaces)) {
			return false;
		}

		return interfaces.stream().anyMatch(itf -> itf.equals(expectInterface));
	}

	/**
	 * Check the annotations of the database type
	 *
	 * @param clazz            Checked class
	 * @param databaseTypeEnum Need to match the database type
	 * @return true: Match
	 * false: Not match
	 */
	public static boolean matchDatabaseTypeAnnotation(Class<?> clazz, DatabaseTypeEnum databaseTypeEnum) {
		if (clazz == null || databaseTypeEnum == null) {
			return false;
		}

		DatabaseTypeAnnotation databaseTypeAnnotation = clazz.getAnnotation(DatabaseTypeAnnotation.class);
		DatabaseTypeAnnotations databaseTypeAnnotations = clazz.getAnnotation(DatabaseTypeAnnotations.class);
		if (databaseTypeAnnotation == null && databaseTypeAnnotations == null) {
			return false;
		}

		if (databaseTypeAnnotation == null) {
			DatabaseTypeAnnotation[] typeAnnotations = databaseTypeAnnotations.value();
			for (DatabaseTypeAnnotation typeAnnotation : typeAnnotations) {
				if (typeAnnotation.type().equals(databaseTypeEnum)) {
					return true;
				}
			}
		} else {
			return databaseTypeAnnotation.type().equals(databaseTypeEnum);
		}

		return false;
	}

	/**
	 * 获取一个数据库实现
	 *
	 * @param databaseType 数据库类型
	 * @param interfaceClz 接口类
	 * @param <T>          接口类型
	 * @return 接口实现
	 * @throws IOException 异常
	 */
	public static <T> T newDatabaseImpl(DatabaseTypeEnum databaseType, Class<T> interfaceClz) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
		Set<Class<?>> matchComponents = BeanUtil.findMatchComponents(interfaceClz.getPackage().getName());
		for (Class c : matchComponents) {
			if (BeanUtil.hasInterface(c, interfaceClz, true)
					&& BeanUtil.matchDatabaseTypeAnnotation(c, databaseType)) {
				Constructor<T> constructor = c.getConstructor();
				return constructor.newInstance();
			}
		}
		return null;
	}
}

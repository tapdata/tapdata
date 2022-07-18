package io.tapdata.common;

import com.tapdata.entity.DatabaseTypeEnum;
import io.tapdata.annotation.DatabaseTypeAnnotation;
import io.tapdata.annotation.DatabaseTypeAnnotations;
import io.tapdata.entity.Converter;
import io.tapdata.entity.Lib;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Pattern;

public class ClassScanner {

	private final static Logger logger = LogManager.getLogger(ClassScanner.class);

	public static final String CLASS_SUFFIX = ".class";
	private static final Pattern INNER_PATTERN = Pattern.compile("\\$(\\d+).", Pattern.CASE_INSENSITIVE);
	private static final String SCAN_PACKAGE_NAME = "io.tapdata";
	private static final String SOURCE_CLASS_NAME = "io.tapdata.Source";
	private static final String TARGET_CLASS_NAME = "io.tapdata.Target";
	private static final String CONVERTER_PACKAGE_NAME = "io.tapdata.converter";
	private static final String CONVERTER_PROVIDER_CLASS_NAME = "io.tapdata.ConverterProvider";

	public static final String SOURCE = "source";
	public static final String TARGET = "target";

	private String buildProfile;

	static List<Lib> libs = new ArrayList<>();
	static List<Converter> converters = new ArrayList<>();

	static List<DatabaseTypeEnum.DatabaseType> databaseTypes = new ArrayList<>();

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

	protected static Enumeration<URL> findAllClassPathResources(String path) throws IOException {
		if (path.startsWith("/")) {
			path = path.substring(1);
		}
		Enumeration<URL> urls = Thread.currentThread().getContextClassLoader().getResources(path);

		return urls;
	}

	public List<Lib> initLibs() throws IOException {
		Set<Class<?>> matchComponents = findMatchComponents(SCAN_PACKAGE_NAME);

		if (CollectionUtils.isNotEmpty(matchComponents)) {
			for (Class<?> matchComponent : matchComponents) {
				if (!matchComponent.isInterface() &&
						(matchComponent.isAnnotationPresent(DatabaseTypeAnnotation.class)
								|| matchComponent.isAnnotationPresent(DatabaseTypeAnnotations.class))) {

					Lib lib = new Lib();
					List<String> databaseTypes = new ArrayList<>();

					// implements source,target or both
					if (!recursiveCheck(matchComponent, lib)) {
						continue;
					}

					// get annotation database type
					Annotation[] databaseTypeAnnotations = matchComponent.getDeclaredAnnotations();
					List<DatabaseTypeAnnotation> annotations = new ArrayList<>();

					if (databaseTypeAnnotations != null && databaseTypeAnnotations.length > 0) {
						for (Annotation annotation : databaseTypeAnnotations) {
							if (annotation instanceof DatabaseTypeAnnotation) {
								DatabaseTypeAnnotation databaseTypeAnnotation = (DatabaseTypeAnnotation) annotation;
								annotations.add(databaseTypeAnnotation);

							} else if (annotation instanceof DatabaseTypeAnnotations) {
								DatabaseTypeAnnotations databaseTypeAnnotations1 = (DatabaseTypeAnnotations) annotation;
								for (DatabaseTypeAnnotation databaseTypeAnnotation : databaseTypeAnnotations1.value()) {
									annotations.add(databaseTypeAnnotation);
								}
							}
						}
					}

					if (CollectionUtils.isNotEmpty(annotations)) {
						for (DatabaseTypeAnnotation databaseTypeAnnotation : annotations) {
							DatabaseTypeEnum type = databaseTypeAnnotation.type();
							if (type != null && StringUtils.isNotBlank(type.getType())) {
								String className = checkLibDatabaseTypeIfExists(type.getType());
								if (StringUtils.isBlank(className)) {
									databaseTypes.add(type.getType());
								} else {
									logger.warn("Found duplication database type [{}] in class [{}], will ignore.", type.getType(), className);
								}
							}
						}

						if (CollectionUtils.isNotEmpty(databaseTypes)) {
							lib.setDatabaseTypes(databaseTypes);
						} else {
							continue;
						}
					} else {
						continue;
					}

					lib.setClazz(matchComponent);

					if (lib.isEmpty()) {
						logger.warn("Failed to init lib, because of missing attributes\n   {}", lib.toString());
					} else {
						libs.add(lib);
					}
				}
			}
		}
		return libs;
	}

	private boolean recursiveCheck(Class<?> matchComponent, Lib lib) {
		Class<?>[] interfaces = matchComponent.getInterfaces();
		if (interfaces != null && interfaces.length > 0) {
			for (Class<?> anInterface : interfaces) {
				if (anInterface.getName().equals(SOURCE_CLASS_NAME)) {
					lib.setSource(true);
				}
				if (anInterface.getName().equals(TARGET_CLASS_NAME)) {
					lib.setTarget(true);
				}
			}
			if (!lib.getSource() && !lib.getTarget()) {
				return false;
			}
		} else if (matchComponent.getSuperclass() != null) {
			return recursiveCheck(matchComponent.getSuperclass(), lib);
		} else {
			return false;
		}
		return true;
	}

	private static String checkLibDatabaseTypeIfExists(String databaseType) {
		if (CollectionUtils.isNotEmpty(libs)) {
			for (Lib lib : libs) {
				List<String> databaseTypes = lib.getDatabaseTypes();
				for (String type : databaseTypes) {
					if (type.equalsIgnoreCase(databaseType)) {
						return lib.getClazz().getName();
					}
				}
			}
		}

		return "";
	}

	public List<DatabaseTypeEnum.DatabaseType> getDatabaseTypes() {
		DatabaseTypeEnum[] values = DatabaseTypeEnum.values();
		if (values != null && values.length > 0) {
			for (DatabaseTypeEnum value : values) {
				List<String> supportTargetByDatabaseType = getSupportTargetByDatabaseType(value);
				String[] buildProfiles = value.getBuildProfiles();
				if (buildProfiles != null && buildProfiles.length > 0) {
					for (String profile : buildProfiles) {
						if (profile.equalsIgnoreCase(this.buildProfile)) {
							databaseTypes.add(new DatabaseTypeEnum.DatabaseType(value.getType(), value.getName(), supportTargetByDatabaseType));
						}
					}
				} else {
					databaseTypes.add(new DatabaseTypeEnum.DatabaseType(value.getType(), value.getName(), supportTargetByDatabaseType));
				}
			}
		}
		return databaseTypes;
	}

	private static List<String> getSupportTargetByDatabaseType(DatabaseTypeEnum databaseTypeEnum) {
		List<String> supports = new ArrayList<>();

		/* all supports */
		supports.add(DatabaseTypeEnum.DUMMY.getType());
		supports.add(DatabaseTypeEnum.BITSFLOW.getType());

		/* specify supports */
		switch (databaseTypeEnum) {
			case ORACLE:
			case MYSQL:
			case MYSQL_PXC:
			case MARIADB:
			case KUNDB:
			case ADB_MYSQL:
			case ALIYUN_MYSQL:
			case ALIYUN_MARIADB:
			case DAMENG:
			case MSSQL:
			case ALIYUN_MSSQL:
			case SYBASEASE:
			case DUMMY:
			case GRIDFS:
			case TCP_UDP:
			case MONGODB:
			case ALIYUN_MONGODB:
			case REST:
			case GBASE8S:
			case CUSTOM:
			case BITSFLOW:
			case DB2:
			case GAUSSDB200:
			case POSTGRESQL:
			case ALIYUN_POSTGRESQL:
			case ADB_POSTGRESQL:
			case GREENPLUM:
			case KUDU:
				supports.add(DatabaseTypeEnum.MONGODB.getType());
				supports.add(DatabaseTypeEnum.ALIYUN_MONGODB.getType());
				supports.add(DatabaseTypeEnum.ORACLE.getType());
				supports.add(DatabaseTypeEnum.MYSQL.getType());
				supports.add(DatabaseTypeEnum.MARIADB.getType());
				supports.add(DatabaseTypeEnum.DAMENG.getType());
				supports.add(DatabaseTypeEnum.KUNDB.getType());
				supports.add(DatabaseTypeEnum.ADB_MYSQL.getType());
				supports.add(DatabaseTypeEnum.ALIYUN_MYSQL.getType());
				supports.add(DatabaseTypeEnum.ALIYUN_MARIADB.getType());

				supports.add(DatabaseTypeEnum.MYSQL_PXC.getType());
				supports.add(DatabaseTypeEnum.MSSQL.getType());
				supports.add(DatabaseTypeEnum.ALIYUN_MSSQL.getType());
				supports.add(DatabaseTypeEnum.TCP_UDP.getType());
				supports.add(DatabaseTypeEnum.SYBASEASE.getType());
				supports.add(DatabaseTypeEnum.GBASE8S.getType());
				supports.add(DatabaseTypeEnum.DB2.getType());
				supports.add(DatabaseTypeEnum.GAUSSDB200.getType());
				supports.add(DatabaseTypeEnum.POSTGRESQL.getType());
				supports.add(DatabaseTypeEnum.ALIYUN_POSTGRESQL.getType());
				supports.add(DatabaseTypeEnum.ADB_POSTGRESQL.getType());
				supports.add(DatabaseTypeEnum.GREENPLUM.getType());
				supports.add(DatabaseTypeEnum.FILE.getType());
				supports.add(DatabaseTypeEnum.ELASTICSEARCH.getType());
				supports.add(DatabaseTypeEnum.MEM_CACHE.getType());
				supports.add(DatabaseTypeEnum.CUSTOM.getType());
				supports.add(DatabaseTypeEnum.KUDU.getType());

				break;

			case FILE:

				supports.add(DatabaseTypeEnum.GRIDFS.getType());
				supports.add(DatabaseTypeEnum.FILE.getType());
				break;
		}

		return supports;
	}

	public static Class<?> getClazzByDatabaseType(String databaseType, String sourceOrTarget) {
		Class<?> clazz = null;
		if (StringUtils.isNotBlank(databaseType)
				&& (StringUtils.isBlank(sourceOrTarget) || sourceOrTarget.equals(SOURCE) || sourceOrTarget.equals(TARGET))) {
			if (CollectionUtils.isNotEmpty(libs)) {
				for (Lib lib : libs) {
					List<String> databaseTypes = lib.getDatabaseTypes();
					for (String type : databaseTypes) {
						if (type.equals(databaseType)) {
							if (StringUtils.isBlank(sourceOrTarget)) {
								clazz = lib.getClazz();
							} else {
								if (sourceOrTarget.equals(SOURCE) && lib.getSource()) {
									clazz = lib.getClazz();
								} else if (sourceOrTarget.equals(TARGET) && lib.getTarget()) {
									clazz = lib.getClazz();
								}
							}

							if (clazz != null) {
								break;
							}
						}
					}

					if (clazz != null) {
						break;
					}
				}
				if (clazz == null) {
					throw new RuntimeException("Database type [ " + databaseType + " ] is not supported, because lib not found.");
				}
			} else {
				throw new RuntimeException("Database type [ " + databaseType + " ] is not supported, because lib list is empty.");
			}
		} else {
			throw new RuntimeException("Input parameters wrong: databaseType = " + databaseType + ", sourceOrTarget = " + sourceOrTarget);
		}

		return clazz;
	}

	public static Class<?> getConverterByDatabaseType(String databaseType) {
		if (CollectionUtils.isNotEmpty(converters)) {
			for (Converter converter : converters) {
				if (converter.getDatabaseType().equals(databaseType)) {
					return converter.getClazz();
				}
			}
		}

		return null;
	}

	public List<Converter> loadConverters() throws IOException {
		Set<Class<?>> matchComponents = findMatchComponents(CONVERTER_PACKAGE_NAME);

		if (CollectionUtils.isNotEmpty(matchComponents)) {
			for (Class<?> matchComponent : matchComponents) {
				if (matchComponent != null && !matchComponent.isInterface() && (matchComponent.isAnnotationPresent(DatabaseTypeAnnotation.class) || matchComponent.isAnnotationPresent(DatabaseTypeAnnotations.class))) {

					if (matchComponent.isAnnotationPresent(DatabaseTypeAnnotation.class)) {
						DatabaseTypeAnnotation annotation = matchComponent.getAnnotation(DatabaseTypeAnnotation.class);
						handleAnnotation(new DatabaseTypeAnnotation[]{annotation}, matchComponent);
					} else if (matchComponent.isAnnotationPresent(DatabaseTypeAnnotations.class)) {
						DatabaseTypeAnnotations annotation = matchComponent.getAnnotation(DatabaseTypeAnnotations.class);
						handleAnnotation(annotation.value(), matchComponent);
					}

				}
			}
		}

		return converters;
	}

	public void handleAnnotation(DatabaseTypeAnnotation[] annotation, Class<?> matchComponent) {

		for (DatabaseTypeAnnotation databaseTypeAnnotation : annotation) {
			Converter converter = new Converter();
			if (databaseTypeAnnotation != null) {
				String type = databaseTypeAnnotation.type().getType();

				converter.setDatabaseType(type);
			} else {
				continue;
			}

			Class<?>[] interfaces = matchComponent.getInterfaces();
			if (interfaces != null && interfaces.length > 0) {
				for (Class<?> anInterface : interfaces) {
					if (anInterface.getName().equals(CONVERTER_PROVIDER_CLASS_NAME)) {
						converter.setClazz(matchComponent);
						break;
					}
				}
			} else {
				continue;
			}

			if (!converter.isEmpty()) {
				converters.add(converter);
			}
		}
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	public static <T> List<Class<? extends T>> getAllClassByInterface(Class<T> clazz) {
		List<Class<? extends T>> list = new ArrayList<>();
		// 判断是否是一个接口
		if (clazz.isInterface()) {
			try {
				List<Class<? extends T>> allClass = getAllClass(clazz.getPackage().getName(), clazz);
				/**
				 * 循环判断路径下的所有类是否实现了指定的接口 并且排除接口类自己
				 */
				for (int i = 0; i < allClass.size(); i++) {
					/**
					 * 判断是不是同一个接口
					 */
					// isAssignableFrom:判定此 Class 对象所表示的类或接口与指定的 Class
					// 参数所表示的类或接口是否相同，或是否是其超类或超接口
					if (clazz.isAssignableFrom((Class<?>) allClass.get(i))) {
						if (!clazz.equals(allClass.get(i))) {
							// 自身并不加进去
							list.add(allClass.get(i));
						}
					}
				}
			} catch (Exception e) {
				System.out.println("出现异常");
			}
		} else {
			// 如果不是接口，则获取它的所有子类
			try {
				List<Class<? extends T>> allClass = getAllClass(clazz.getPackage().getName(), clazz);
				/**
				 * 循环判断路径下的所有类是否继承了指定类 并且排除父类自己
				 */
				for (int i = 0; i < allClass.size(); i++) {
					if (clazz.isAssignableFrom((Class<?>) allClass.get(i))) {
						if (!clazz.equals(allClass.get(i))) {
							// 自身并不加进去
							list.add(allClass.get(i));
						}
					}
				}
			} catch (Exception e) {
				System.out.println("出现异常");
			}
		}
		return list;
	}

	/**
	 * 从一个指定路径下查找所有的类
	 */
	@SuppressWarnings("rawtypes")
	private static <T> List<Class<? extends T>> getAllClass(String packagename, Class<T> classz) {
		ArrayList<Class<? extends T>> list = new ArrayList<>();
		// 返回对当前正在执行的线程对象的引用。
		// 返回该线程的上下文 ClassLoader。
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		String path = packagename.replace('.', '/');
		try {
			ArrayList<File> fileList = new ArrayList<>();
			/**
			 * 这里面的路径使用的是相对路径 如果大家在测试的时候获取不到，请理清目前工程所在的路径 使用相对路径更加稳定！
			 * 另外，路径中切不可包含空格、特殊字符等！
			 */
			// getResources:查找所有给定名称的资源
			// 获取jar包中的实现类:Enumeration<URL> enumeration =
			// classLoader.getResources(path);
			Enumeration<URL> enumeration = classLoader.getResources(path);
			while (enumeration.hasMoreElements()) {
				URL url = enumeration.nextElement();
				// 获取此 URL 的文件名
				fileList.add(new File(url.getFile()));
			}
			for (int i = 0; i < fileList.size(); i++) {
				list.addAll(findClass(fileList.get(i), packagename));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return list;
	}

	/**
	 * 如果file是文件夹，则递归调用findClass方法，或者文件夹下的类 如果file本身是类文件，则加入list中进行保存，并返回
	 *
	 * @param file
	 * @param packagename
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	private static <T> ArrayList<T> findClass(File file, String packagename) {
		ArrayList<T> list = new ArrayList<>();
		if (!file.exists()) {
			return list;
		}
		// 返回一个抽象路径名数组，这些路径名表示此抽象路径名表示的目录中的文件。
		File[] files = file.listFiles();
		for (File file2 : files) {
			if (file2.isDirectory()) {
				// assert !file2.getName().contains(".");// 添加断言用于判断
				if (!file2.getName().contains(".")) {
					ArrayList<T> arrayList = findClass(file2, packagename + "." + file2.getName());
					list.addAll(arrayList);
				}
			} else if (file2.getName().endsWith(".class")) {
				try {
					// 保存的类文件不需要后缀.class
					list.add((T) Class.forName(packagename + '.' + file2.getName().substring(0, file2.getName().length() - 6)));
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
		}
		return list;
	}

	public String getBuildProfile() {
		return buildProfile;
	}

	public void setBuildProfile(String buildProfile) {
		this.buildProfile = buildProfile;
	}
}

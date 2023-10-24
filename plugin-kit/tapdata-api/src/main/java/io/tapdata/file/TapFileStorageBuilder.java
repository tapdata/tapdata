package io.tapdata.file;

import io.tapdata.entity.error.CoreException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

public class TapFileStorageBuilder {
	private Map<String, Object> params;
	public TapFileStorageBuilder withParams(Map<String, Object> params) {
		this.params = params;
		return this;
	}
	private String storageClassName;
	public TapFileStorageBuilder withStorageClassName(String storageClassName) {
		this.storageClassName = storageClassName;
		return this;
	}
	private ClassLoader classLoader;
	public TapFileStorageBuilder withClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
		return this;
	}

	public TapFileStorage build() throws Exception {
		if(storageClassName == null)
			throw new CoreException(FileErrors.MISSING_STORAGE_CLASS_NAME, "Missing storage class name");

		Class<?> storageClass = null;
		try {
			if(classLoader != null)
				storageClass = classLoader.loadClass(storageClassName);
			else
				storageClass = Class.forName(storageClassName);
		} catch (ClassNotFoundException e) {
			if(classLoader != null)
				try {
					storageClass = Class.forName(storageClassName);
				} catch (ClassNotFoundException ignored) {
				}
			if(storageClass == null) {
				throw new CoreException(FileErrors.CLASS_NOT_FOUND, "storageClassName {} not found in classLoader {}", storageClassName, classLoader);
			}
		}
		if(!TapFileStorage.class.isAssignableFrom(storageClass)) {
			throw new CoreException(FileErrors.NEED_IMPLEMENT_TAP_FILE_STORAGE, "Need implement TapFileStorage");
		}
		TapFileStorage fileStorage = null;
		try {
			fileStorage = (TapFileStorage) storageClass.getConstructor().newInstance();
		} catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
			throw new CoreException(FileErrors.NEW_INSTANCE_FAILED, "New instance {} failed, {}", storageClass, e.getMessage()).cause(e);
		}
		fileStorage.init(params);
		return fileStorage;
	}
}

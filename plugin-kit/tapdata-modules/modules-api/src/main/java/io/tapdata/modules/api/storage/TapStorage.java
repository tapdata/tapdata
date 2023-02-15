package io.tapdata.modules.api.storage;

/**
 * @author aplomb
 */
public interface TapStorage {
	void init(String id, TapStorageFactory.StorageOptions storageOptions);
	void setClassLoader(ClassLoader classLoader);
	void clear();
	void destroy();

	void setPath(String path);
}

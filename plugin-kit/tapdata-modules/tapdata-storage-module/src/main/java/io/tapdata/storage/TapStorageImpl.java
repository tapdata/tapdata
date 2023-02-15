package io.tapdata.storage;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.ObjectSerializable;
import io.tapdata.modules.api.storage.TapStorage;
import io.tapdata.modules.api.storage.TapStorageFactory;
import io.tapdata.storage.errors.StorageErrors;
import org.apache.commons.lang3.StringUtils;

/**
 * @author aplomb
 */
public abstract class TapStorageImpl implements TapStorage {
	public static final String STATE_NONE = "none";
	public static final String STATE_INITIALIZING = "initializing";
	public static final String STATE_INITIALIZED = "initialized";
	public static final String STATE_DESTROYED = "destroyed";
	@Bean
	protected ObjectSerializable objectSerializable;
	protected String id;
	protected TapStorageFactory.StorageOptions storageOptions;
	protected ClassLoader classLoader;
	@Override
	public void setClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	protected String path;
	@Override
	public void setPath(String path) {
		this.path = path;
	}
	protected void initState(String id, TapStorageFactory.StorageOptions storageOptions) {
		this.id = id;
		if(StringUtils.isBlank(this.id)) {
			throw new CoreException(StorageErrors.ILLEGAL_ARGUMENT, "Missing id");
		}
		this.storageOptions = storageOptions;
		if(StringUtils.isBlank(this.storageOptions.getRootPath())) {
			throw new CoreException(StorageErrors.ILLEGAL_ARGUMENT, "Missing rootPath");
		}
	}

}

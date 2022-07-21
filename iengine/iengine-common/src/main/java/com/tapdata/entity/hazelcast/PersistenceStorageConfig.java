package com.tapdata.entity.hazelcast;

import com.hazelcast.persistence.StorageMode;

import java.io.Serializable;

/**
 * @author samuel
 * @Description
 * @create 2022-02-18 16:40
 **/
public class PersistenceStorageConfig implements Serializable {

	private static final long serialVersionUID = -1937587096018270201L;

	private StorageMode storageMode;
	private String rocksDBPath;
	private String mongoUri;
	private String mongoDbName;
	private String mongoCollection;
	private Integer inMemSize;
	private int shareCdcTtlDay;
	private boolean firstTime = true;
	private boolean enable = false;
	private Throwable throwable;

	private PersistenceStorageConfig() {
	}

	public static PersistenceStorageConfig getInstance() {
		return PersistenceStorageConfigSingleton.INSTANCE.getInstance();
	}

	private enum PersistenceStorageConfigSingleton {
		INSTANCE;

		private final PersistenceStorageConfig persistenceStorageConfig;

		PersistenceStorageConfigSingleton() {
			persistenceStorageConfig = new PersistenceStorageConfig();
		}

		private PersistenceStorageConfig getInstance() {
			return persistenceStorageConfig;
		}
	}

	public StorageMode getStorageMode() {
		return storageMode;
	}

	public void setStorageMode(StorageMode storageMode) {
		this.storageMode = storageMode;
	}

	public String getRocksDBPath() {
		return rocksDBPath;
	}

	public void setRocksDBPath(String rocksDBPath) {
		this.rocksDBPath = rocksDBPath;
	}

	public String getMongoUri() {
		return mongoUri;
	}

	public void setMongoUri(String mongoUri) {
		this.mongoUri = mongoUri;
	}

	public String getMongoDbName() {
		return mongoDbName;
	}

	public void setMongoDbName(String mongoDbName) {
		this.mongoDbName = mongoDbName;
	}

	public String getMongoCollection() {
		return mongoCollection;
	}

	public void setMongoCollection(String mongoCollection) {
		this.mongoCollection = mongoCollection;
	}

	public Integer getInMemSize() {
		return inMemSize;
	}

	public void setInMemSize(Integer inMemSize) {
		this.inMemSize = inMemSize;
	}

	public boolean isFirstTime() {
		return firstTime;
	}

	public void setFirstTime(boolean firstTime) {
		this.firstTime = firstTime;
	}

	public boolean isEnable() {
		return enable;
	}

	public void setEnable(boolean enable) {
		this.enable = enable;
	}

	public Throwable getThrowable() {
		return throwable;
	}

	public void setThrowable(Throwable throwable) {
		this.throwable = throwable;
	}

	public int getShareCdcTtlDay() {
		return shareCdcTtlDay;
	}

	public void setShareCdcTtlDay(int shareCdcTtlDay) {
		this.shareCdcTtlDay = shareCdcTtlDay;
	}

	public static void clearMongoConfig() {
		PersistenceStorageConfig persistenceStorageConfig = PersistenceStorageConfig.getInstance();
		persistenceStorageConfig.setMongoUri("");
		persistenceStorageConfig.setMongoDbName("");
		persistenceStorageConfig.setMongoCollection("");
	}

	public static void clearRocksDbConfig() {
		PersistenceStorageConfig.getInstance().setRocksDBPath("");
	}
}

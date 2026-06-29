package com.tapdata.tm.commons.externalStorage;

/**
 * @author samuel
 * @Description
 * @create 2022-09-07 12:28
 **/
public enum ExternalStorageType {
	memory("Mem"),
	mongodb("MongoDB", true),
	rocksdb("RocksDB"),
	httptm("HttpTM"),
	;
	private final String mode;
	private final boolean supportChangeLog;

	ExternalStorageType(String mode) {
		this(mode, false);
	}

	ExternalStorageType(String mode, boolean supportChangeLog) {
		this.mode = mode;
		this.supportChangeLog = supportChangeLog;
	}

	public String getMode() {
		return mode;
	}

	public static boolean supported(String mode) {
		for (ExternalStorageType value : values()) {
			if (!value.supportChangeLog) {
				continue;
			}
			if (value.name().equals(mode)) {
				return true;
			}
		}
		return false;
	}
}

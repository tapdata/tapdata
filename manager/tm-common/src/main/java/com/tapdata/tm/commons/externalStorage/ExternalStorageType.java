package com.tapdata.tm.commons.externalStorage;

/**
 * @author samuel
 * @Description
 * @create 2022-09-07 12:28
 **/
public enum ExternalStorageType {
	memory("Mem"),
	mongodb("MongoDB"),
	rocksdb("RocksDB"),
	httptm("HttpTM"),
	;
	private String mode;

	ExternalStorageType(String mode) {
		this.mode = mode;
	}

	public String getMode() {
		return mode;
	}
}

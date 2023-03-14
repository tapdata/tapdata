package io.tapdata.pdk.apis.functions.connection;

/**
 * @author aplomb
 */
public class TableInfo {
	public static TableInfo create() {
		return new TableInfo();
	}
	private Long numOfRows;
	public TableInfo numOfRows(Long numOfRows) {
		this.numOfRows = numOfRows;
		return this;
	}

	private Long storageSize;
	public TableInfo storageSize(Long storageSize) {
		this.storageSize = storageSize;
		return this;
	}

	public Long getNumOfRows() {
		return numOfRows;
	}

	public void setNumOfRows(Long numOfRows) {
		this.numOfRows = numOfRows;
	}

	public Long getStorageSize() {
		return storageSize;
	}

	public void setStorageSize(Long storageSize) {
		this.storageSize = storageSize;
	}
}

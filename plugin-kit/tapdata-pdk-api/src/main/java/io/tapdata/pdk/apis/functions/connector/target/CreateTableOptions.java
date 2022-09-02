package io.tapdata.pdk.apis.functions.connector.target;

public class CreateTableOptions {
	public static CreateTableOptions create() {
		return new CreateTableOptions();
	}
	private Boolean tableExists;
	public CreateTableOptions tableExists(boolean tableExists) {
		this.tableExists = tableExists;
		return this;
	}

	public Boolean getTableExists() {
		return tableExists;
	}

	public void setTableExists(Boolean tableExists) {
		this.tableExists = tableExists;
	}
}

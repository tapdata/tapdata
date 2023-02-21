package io.tapdata.pdk.apis.functions.connection;

/**
 * @author aplomb
 */
public class CheckTableNameResult {
	public static CheckTableNameResult create() {
		return new CheckTableNameResult();
	}
	/**
	 * Whether the table name is supported or not
	 */
	private boolean supported;
	public CheckTableNameResult supported(boolean supported) {
		this.supported = supported;
		return this;
	}

	/**
	 * The reason why the table name is unsupported
	 */
	public String unSupportReason;
	public CheckTableNameResult unSupportReason(String unSupportReason) {
		this.unSupportReason = unSupportReason;
		return this;
	}

	/**
	 * The rule of table name
	 */
	public String tableNameRule;
	public CheckTableNameResult tableNameRule(String tableNameRule) {
		this.tableNameRule = tableNameRule;
		return this;
	}

	public boolean isSupported() {
		return supported;
	}

	public void setSupported(boolean supported) {
		this.supported = supported;
	}

	public String getUnSupportReason() {
		return unSupportReason;
	}

	public void setUnSupportReason(String unSupportReason) {
		this.unSupportReason = unSupportReason;
	}

	public String getTableNameRule() {
		return tableNameRule;
	}

	public void setTableNameRule(String tableNameRule) {
		this.tableNameRule = tableNameRule;
	}


}

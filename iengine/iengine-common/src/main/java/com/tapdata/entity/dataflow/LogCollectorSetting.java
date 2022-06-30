package com.tapdata.entity.dataflow;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2020-07-24 16:40
 **/
public class LogCollectorSetting implements Serializable {

	public final static String ALLTABLES = "allTables";
	public final static String RESERVATIONTABLE = "reservationTable";
	public final static String EXCLUSIONTABLE = "exclusionTable";

	private static final long serialVersionUID = 2195847261526378052L;
	private String connectionId;
	private List<String> includeTables;
	private String selectType;

	public LogCollectorSetting() {

	}

	public String getConnectionId() {
		return connectionId;
	}

	public void setConnectionId(String connectionId) {
		this.connectionId = connectionId;
	}

	public List<String> getIncludeTables() {
		return includeTables;
	}

	public void setIncludeTables(List<String> includeTables) {
		this.includeTables = includeTables;
	}

	public String getSelectType() {
		return selectType;
	}

	public void setSelectType(String selectType) {
		this.selectType = selectType;
	}

	public boolean isEmpty() {
		return StringUtils.isAnyBlank(connectionId, selectType)
				|| CollectionUtils.isEmpty(includeTables);
	}
}

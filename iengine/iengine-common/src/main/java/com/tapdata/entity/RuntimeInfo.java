package com.tapdata.entity;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by xj
 * 2020-03-06 21:33
 **/
public class RuntimeInfo implements Serializable {

	private static final long serialVersionUID = -6516583281707633407L;
	private boolean ddlConfirm = true;

	private List<UnSupportedDDL> unSupportedDDLS;

	private List<Map<String, Object>> timeoutTxns;

	public RuntimeInfo() {
	}

	public RuntimeInfo(List<Map<String, Object>> timeoutTxns) {
		this.ddlConfirm = true;
		this.timeoutTxns = timeoutTxns;
	}

	public RuntimeInfo(boolean ddlConfirm, List<UnSupportedDDL> unSupportedDDLS) {
		this.ddlConfirm = ddlConfirm;
		this.unSupportedDDLS = unSupportedDDLS;
	}

	public boolean getDdlConfirm() {
		return ddlConfirm;
	}

	public void setDdlConfirm(boolean ddlConfirm) {
		this.ddlConfirm = ddlConfirm;
	}

	public List<UnSupportedDDL> getUnSupportedDDLS() {
		return unSupportedDDLS;
	}

	public void setUnSupportedDDLS(List<UnSupportedDDL> unSupportedDDLS) {
		this.unSupportedDDLS = unSupportedDDLS;
	}

	public List<Map<String, Object>> getTimeoutTxns() {
		return timeoutTxns;
	}

	public void setTimeoutTxns(List<Map<String, Object>> timeoutTxns) {
		this.timeoutTxns = timeoutTxns;
	}
}

package com.tapdata.processor.context;

public class GridfsScriptContext extends ProcessContext {

	private String filename;

	private String createtime;

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public String getCreatetime() {
		return createtime;
	}

	public void setCreatetime(String createtime) {
		this.createtime = createtime;
	}
}

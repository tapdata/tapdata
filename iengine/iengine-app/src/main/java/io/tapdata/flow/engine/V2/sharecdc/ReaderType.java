package io.tapdata.flow.engine.V2.sharecdc;

/**
 * @author samuel
 * @Description
 * @create 2022-02-21 17:07
 **/
public enum ReaderType {
	PDK_TASK_HAZELCAST("io.tapdata.flow.engine.V2.sharecdc.impl.ShareCdcPDKTaskReader"),
	;

	private String clazz;

	ReaderType(String clazz) {
		this.clazz = clazz;
	}

	public String getClazz() {
		return clazz;
	}
}

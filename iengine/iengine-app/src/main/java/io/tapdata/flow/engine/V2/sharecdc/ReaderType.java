package io.tapdata.flow.engine.V2.sharecdc;

import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2022-02-21 17:07
 **/
public enum ReaderType {
	PDK_TASK_HAZELCAST("io.tapdata.flow.engine.V2.sharecdc.impl.ShareCdcPDKTaskReader", new Class[]{Object.class}),
	;

	private String clazz;
	private Class<?>[] classes;

	ReaderType(String clazz) {
		this.clazz = clazz;
	}

	ReaderType(String clazz, Class<?>[] classes) {
		this.clazz = clazz;
		this.classes = classes;
	}

	public String getClazz() {
		return clazz;
	}

	public Class<?>[] getClasses() {
		return classes;
	}
}

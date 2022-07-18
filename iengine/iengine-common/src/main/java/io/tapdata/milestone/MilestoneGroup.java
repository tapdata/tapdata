package io.tapdata.milestone;

/**
 * @author samuel
 * @Description
 * @create 2021-12-09 12:00
 **/
public enum MilestoneGroup {
	INIT("init"),
	STRUCTURE("structure"),
	INITIAL_SYNC("initial_sync"),
	CDC("cdc"),
	;

	private String name;

	MilestoneGroup(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}
}

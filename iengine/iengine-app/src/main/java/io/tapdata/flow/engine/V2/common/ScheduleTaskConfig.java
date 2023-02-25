package io.tapdata.flow.engine.V2.common;

/**
 * @author samuel
 * @Description
 * @create 2022-11-23 10:47
 **/
public abstract class ScheduleTaskConfig {
	private String name;

	public ScheduleTaskConfig(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	@Override
	public String toString() {
		return "ScheduleTaskConfig{" +
				"name='" + name + '\'' +
				'}';
	}
}

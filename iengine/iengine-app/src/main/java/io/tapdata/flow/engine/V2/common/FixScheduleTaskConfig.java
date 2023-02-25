package io.tapdata.flow.engine.V2.common;

/**
 * @author samuel
 * @Description
 * @create 2022-11-23 10:49
 **/
public class FixScheduleTaskConfig extends ScheduleTaskConfig {
	private long fixedDelay;

	private FixScheduleTaskConfig(String name) {
		super(name);
	}

	public static FixScheduleTaskConfig create(String name, long fixedDelay) {
		return new FixScheduleTaskConfig(name).fixedDelay(fixedDelay);
	}

	public FixScheduleTaskConfig fixedDelay(long fixedDelay) {
		this.fixedDelay = fixedDelay;
		return this;
	}

	public long getFixedDelay() {
		return fixedDelay;
	}

	@Override
	public String toString() {
		return "FixScheduleTaskConfig{" +
				"fixedDelay=" + fixedDelay +
				"} " + super.toString();
	}
}

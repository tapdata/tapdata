package io.tapdata.wsserver.channels.health;

public interface HealthWeightListener {
	/**
	 * Range is 0~100
	 *
	 * @return
	 */
	int weight();

	/**
	 * Range is -1 ~ 10000
	 *
	 * -1 is can not accept any.
	 *
	 * @return
	 */
	int health();
}

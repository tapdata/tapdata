package io.tapdata.metric;

public class PredicateResult {

	private final boolean health;
	private final String detail;

	public PredicateResult(boolean health, String detail) {
		this.health = health;
		this.detail = detail;
	}

	public boolean isHealth() {
		return health;
	}

	public String getDetail() {
		return detail;
	}

}

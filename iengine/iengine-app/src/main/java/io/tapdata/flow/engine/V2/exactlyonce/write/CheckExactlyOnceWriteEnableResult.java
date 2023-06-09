package io.tapdata.flow.engine.V2.exactlyonce.write;

/**
 * @author samuel
 * @Description
 * @create 2023-05-18 15:59
 **/
public class CheckExactlyOnceWriteEnableResult {
	private Boolean enable = false;
	private String message;

	private CheckExactlyOnceWriteEnableResult() {
	}

	public static CheckExactlyOnceWriteEnableResult createEnable() {
		return new CheckExactlyOnceWriteEnableResult()
				.enable(true);
	}

	public static CheckExactlyOnceWriteEnableResult createDisable(String disableMessage) {
		return new CheckExactlyOnceWriteEnableResult()
				.message(disableMessage);
	}

	public CheckExactlyOnceWriteEnableResult enable(Boolean enable) {
		this.enable = enable;
		return this;
	}

	public CheckExactlyOnceWriteEnableResult message(String message) {
		this.message = message;
		return this;
	}

	public Boolean getEnable() {
		return enable;
	}

	public String getMessage() {
		return message;
	}
}

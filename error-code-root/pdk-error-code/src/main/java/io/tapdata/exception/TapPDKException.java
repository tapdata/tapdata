package io.tapdata.exception;

/**
 * @author samuel
 * @Description
 * @create 2023-02-23 14:46
 **/
public abstract class TapPDKException extends TapCodeException {
	private static final long serialVersionUID = 4820551931387403402L;

	public TapPDKException(String code, Throwable cause) {
		super(code, cause);
	}
}

package base.ex;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/11/24 14:27 Create
 */
public class TestException extends RuntimeException {
	public TestException() {
		super("throw exception test");
	}

	public TestException(String message) {
		super(message);
	}
}

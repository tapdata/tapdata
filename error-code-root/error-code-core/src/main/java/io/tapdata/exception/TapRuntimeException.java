package io.tapdata.exception;

import java.io.IOException;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Optional;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/11/29 14:16 Create
 */
public abstract class TapRuntimeException extends RuntimeException implements Cloneable {
	private static final long serialVersionUID = 8393742410956400472L;
	public static final int SIMPLE_STACK_TRACE_LINE_LIMIT = 5;

	protected TapRuntimeException() {
	}

	protected TapRuntimeException(String message) {
		super(message);
	}

	protected TapRuntimeException(String message, Throwable cause) {
		super(message, cause);
	}

	protected TapRuntimeException(Throwable cause) {
		super(cause);
	}

	public String simpleStack() {
		Throwable th = this;
		StringBuilder buf = new StringBuilder();
		List<Throwable> handledEx = new ArrayList<>();
		while (!Thread.currentThread().isInterrupted()) {
			if (null == th) {
				if (buf.length() <= 0) {
					th = handledEx.get(handledEx.size() - 1);
					append(th, buf);
				}
				break;
			}
			if (handledEx.contains(th)) {
				break;
			}
			if (th instanceof NullPointerException
					|| th instanceof ArrayIndexOutOfBoundsException
					|| th instanceof NumberFormatException
					|| th instanceof IOException
					|| th instanceof ConcurrentModificationException
					|| th instanceof InterruptedException
					|| th instanceof UnsupportedOperationException) {
				append(th, buf);
			}
			handledEx.add(th);
			th = th.getCause();
		}

		int max = buf.length() - 1;
		for (; max >= 0; max--) {
			if (Optional.of(buf.charAt(max)).map(c -> {
				switch (c) {
					case ';':
					case '\n':
						return false;
					default:
						return true;
				}
			}).get()) break;
		}
		buf.setLength(max + 1);

		return buf.toString();
	}

	private static void append(Throwable th, StringBuilder buf) {
		int index = 0;
		buf.append("Caused by: ").append(th.getClass().getName());
		if (null != th.getMessage() || !"".equals(th.getMessage())) {
			buf.append(": ").append(th.getMessage());
		}
		for (StackTraceElement t : th.getStackTrace()) {
			if (++index > SIMPLE_STACK_TRACE_LINE_LIMIT) {
				buf.append("\n\t...");
				break;
			}
			buf.append("\n\t").append(t);
		}
		buf.append("\n");
	}

	protected void clone(TapRuntimeException tapRuntimeException) {
		// do nothing
	}

	@Override
	public Object clone() {
		Object clone;
		try {
			clone = super.clone();
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
		if (clone instanceof TapRuntimeException) {
			TapRuntimeException tapRuntimeException = (TapRuntimeException) clone;
			clone(tapRuntimeException);
			return tapRuntimeException;
		} else {
			return clone;
		}
	}
}

package io.tapdata.exception;

import java.util.Optional;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/11/29 14:16 Create
 */
public abstract class TapRuntimeException extends RuntimeException {
    public TapRuntimeException() {
    }

    public TapRuntimeException(String message) {
        super(message);
    }

    public TapRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public TapRuntimeException(Throwable cause) {
        super(cause);
    }

    public String stackTrace2String() {
        Throwable th = this;
        StringBuilder buf = new StringBuilder();
        do {
            if (th instanceof TapCodeException) {
                buf.append(th).append("\n");
            } else if (th instanceof TapRuntimeException) {
                buf.append(th);
                for (StackTraceElement t : th.getStackTrace()) {
                    buf.append(t).append(";");
                }
                buf.append("\n");
            } else if (th instanceof NullPointerException || th instanceof ArrayIndexOutOfBoundsException) {
                buf.append(th.getClass().getSimpleName()).append(":");
                for (StackTraceElement t : th.getStackTrace()) {
                    buf.append(t).append(";");
                }
                buf.append("\n");
            }
            th = th.getCause();
        } while (null != th);

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
}

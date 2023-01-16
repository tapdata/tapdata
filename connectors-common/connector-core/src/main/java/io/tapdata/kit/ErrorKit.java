package io.tapdata.kit;

public class ErrorKit {

    public static void ignoreAnyError(AnyError runnable) {
        try {
            runnable.run();
        } catch (Throwable ignored) {
        }
    }

    public static Throwable getLastCause(Throwable e) {
        Throwable last = e;
        while(EmptyKit.isNotNull(last.getCause())) {
            last = last.getCause();
        }
        return last;
    }

    public interface AnyError {
        void run() throws Throwable;
    }
}

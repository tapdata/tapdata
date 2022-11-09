package io.tapdata.kit;

public class ErrorKit {

    public static void ignoreAnyError(AnyError runnable) {
        try {
            runnable.run();
        } catch (Throwable ignored) {
        }
    }

    public interface AnyError {
        void run() throws Throwable;
    }
}

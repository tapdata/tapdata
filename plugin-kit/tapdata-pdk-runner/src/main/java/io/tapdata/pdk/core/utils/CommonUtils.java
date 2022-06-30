package io.tapdata.pdk.core.utils;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.error.CoreException;
import io.tapdata.pdk.core.error.PDKRunnerErrorCodes;
import io.tapdata.pdk.core.error.QuiteException;
import io.tapdata.pdk.core.executor.ExecutorsManager;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class CommonUtils {
    static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
    public static String dateString() {
        return dateString(new Date());
    }
    public static String dateString(Date date) {
        return sdf.format(date);
    }
    public static String uuid() {
        return UUID.randomUUID().toString().replace("-", "");
    }
    public static int getJavaVersion() {
        String version = System.getProperty("java.version");
        if(version.startsWith("1.")) {
            version = version.substring(2, 3);
        } else {
            int dot = version.indexOf(".");
            if(dot != -1) { version = version.substring(0, dot); }
        } return Integer.parseInt(version);
    }

    public interface AnyError {
        void run() throws Throwable;
    }

    public static void autoRetryAsync(AnyError runnable, String tag, String message, long times, long periodSeconds) {
        try {
            runnable.run();
        } catch(Throwable throwable) {
            TapLogger.error(tag, "AutoRetryAsync error {}, execute message {}, retry times {}, periodSeconds {}. ", throwable.getMessage(), message, times, periodSeconds);
            if(times > 0) {
                ExecutorsManager.getInstance().getScheduledExecutorService().schedule(() -> {
                    autoRetryAsync(runnable, tag, message, times - 1, periodSeconds);
                }, periodSeconds, TimeUnit.SECONDS);
            } else {
                if(throwable instanceof CoreException) {
                    throw (CoreException) throwable;
                }
                throw new CoreException(PDKRunnerErrorCodes.COMMON_UNKNOWN, message + " execute failed, " + throwable.getMessage());
            }
        }
    }

    public static void ignoreAnyError(AnyError runnable, String tag) {
        try {
            runnable.run();
        } catch(CoreException coreException) {
            coreException.printStackTrace();
            TapLogger.error(tag, "Error code {} message {} will be ignored. ", coreException.getCode(), coreException.getMessage());
        } catch(Throwable throwable) {
            if(!(throwable instanceof QuiteException)) {
                throwable.printStackTrace();
                TapLogger.error(tag, "Unknown error message {} will be ignored. ", throwable.getMessage());
            }
        }
    }

    private static AtomicLong counter = new AtomicLong(0);

    /**
     * A lot faster than UUID.
     *
     * 1000000 UUID takes 1089, this takes 139
     *
     * @return
     */
    public static String processUniqueId() {
        return Long.toHexString(System.currentTimeMillis()) + Long.toHexString(counter.getAndIncrement());
    }


    public static void handleAnyError(AnyError r) {
        try {
            r.run();
        } catch(CoreException coreException) {
            throw coreException;
        } catch(Throwable throwable) {
            throw new CoreException(PDKRunnerErrorCodes.COMMON_UNKNOWN, throwable.getMessage(), throwable);
        }
    }

    public static void logError(String logTag, String prefix, Throwable throwable) {
        TapLogger.error(logTag, errorMessage(prefix, throwable));
    }

    public static String errorMessage(String prefix, Throwable throwable) {
        if(throwable instanceof CoreException) {
            CoreException coreException = (CoreException) throwable;
            StringBuilder builder = new StringBuilder(prefix).append(",");
            builder.append(" code ").append(coreException.getCode()).append(" message ").append(coreException.getMessage());
            List<CoreException> moreExceptions = coreException.getMoreExceptions();
            if(moreExceptions != null) {
                builder.append(", more errors,");
                for(CoreException coreException1 : moreExceptions) {
                    builder.append(" code ").append(coreException1.getCode()).append(" message ").append(coreException1.getMessage()).append(";");
                }
            }
            return builder.toString();
        } else {
            return prefix + ", unknown error " + throwable.getMessage();
        }
    }

    public static CoreException generateCoreException(Throwable throwable) {
        if (throwable instanceof CoreException) {
            return (CoreException) throwable;
        } else {
            Throwable cause = throwable.getCause();
            if (cause != null && cause instanceof CoreException) {
                return (CoreException) cause;
            }
        }
        return new CoreException(PDKRunnerErrorCodes.COMMON_UNKNOWN, throwable.getMessage(), throwable);
    }

    public static String getProperty(String key, String defaultValue) {
        String value = System.getProperty(key);
        if(value == null)
            value = System.getenv(key);
        if(value == null)
            value = defaultValue;
        return value;
    }

    public static boolean getPropertyBool(String key, boolean defaultValue) {
        String value = System.getProperty(key);
        if(value == null)
            value = System.getenv(key);
        Boolean valueBoolean = null;
        if(value != null) {
            try {
                valueBoolean = Boolean.parseBoolean(value);
            } catch(Throwable ignored) {}
        }
        if(valueBoolean == null)
            valueBoolean = defaultValue;
        return valueBoolean;
    }

    public static int getPropertyInt(String key, int defaultValue) {
        String value = System.getProperty(key);
        if(value == null)
            value = System.getenv(key);
        Integer valueInt = null;
        if(value != null) {
            try {
                valueInt = Integer.parseInt(value);
            } catch(Throwable ignored) {}
        }
        if(valueInt == null)
            valueInt = defaultValue;
        return valueInt;
    }

    public static long getPropertyLong(String key, long defaultValue) {
        String value = System.getProperty(key);
        if(value == null)
            value = System.getenv(key);
        Long valueLong = null;
        if(value != null) {
            try {
                valueLong = Long.parseLong(value);
            } catch(Throwable ignored) {}
        }
        if(valueLong == null)
            valueLong = defaultValue;
        return valueLong;
    }

    public static void setProperty(String key, String value) {
        System.setProperty(key, value);
    }

    public static void main(String[] args) {
        AtomicLong counter = new AtomicLong();

        int times = 2000000;
        long time = System.currentTimeMillis();
        for(int i = 0; i < times; i++) {
            Runnable r = new Runnable() {
                @Override
                public void run() {
                    counter.incrementAndGet();
                }
            };
            r.run();
        }
        System.out.println("1takes " + (System.currentTimeMillis() - time));

        time = System.currentTimeMillis();
        for(int i = 0; i < times; i++) {
            Runnable r = () -> counter.incrementAndGet();
            r.run();
        }
        System.out.println("2takes " + (System.currentTimeMillis() - time));
    }
}

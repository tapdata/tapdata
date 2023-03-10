package io.tapdata.pdk.core.utils;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.functions.ConnectionFunctions;
import io.tapdata.pdk.apis.functions.connection.ErrorHandleFunction;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectionNode;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.Node;
import io.tapdata.pdk.core.error.PDKRunnerErrorCodes;
import io.tapdata.pdk.core.error.QuiteException;
import io.tapdata.pdk.core.executor.ExecutorsManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.io.InputStream;
import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class CommonUtils {
    static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
    public static String dateString() {
        return dateString(new Date());
    }
    public static String dateString(Date date) {
        return sdf.format(date);
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
    public boolean pdkEquals(Node pdkNode, TapEvent event) {
        return pdkEquals(pdkNode, event, true);
    }
    public boolean pdkEquals(Node pdkNode, TapEvent event, boolean ignoreVersion) {
        if(pdkNode != null &&
                pdkNode.getTapNodeInfo() != null &&
                pdkNode.getTapNodeInfo().getTapNodeSpecification() != null &&
                event != null &&
                event.getPdkId() != null &&
                event.getPdkGroup() != null &&
                event.getPdkVersion() != null
        ) {
            TapNodeSpecification specification = pdkNode.getTapNodeInfo().getTapNodeSpecification();
            if(specification.getId() != null && specification.getGroup() != null && specification.getVersion() != null) {
                return specification.getId().equals(event.getPdkId()) && specification.getGroup().equals(event.getPdkGroup()) &&
                        (ignoreVersion || specification.getVersion().equals(event.getPdkVersion()));
            }
        }
        return false;
    }

    public interface AnyError {
        void run() throws Throwable;
    }

    public interface AnyErrorConsumer<T>{
        void accept(T t) throws Throwable;
    }

    public static void awakenRetryObj(Object syncWaitObj) {
        if(syncWaitObj != null) {
            synchronized (syncWaitObj) {
                syncWaitObj.notifyAll();
            }
        }
    }

    protected static void prepareFunctionAndContextForNode(Node node,FunctionAndContext functionAndContext){
        ErrorHandleFunction function = null;
        TapConnectionContext tapConnectionContext = null;
        ConnectionFunctions<?> connectionFunctions;
        if (node instanceof ConnectionNode) {
            ConnectionNode connectionNode = (ConnectionNode) node;
            connectionFunctions = connectionNode.getConnectionFunctions();
            if (null != connectionFunctions) {
                function = connectionFunctions.getErrorHandleFunction();
            } else {
                throw new CoreException("ConnectionFunctions must be not null,connectionNode does not contain ConnectionFunctions");
            }
            tapConnectionContext = connectionNode.getConnectionContext();
        } else if (node instanceof ConnectorNode) {
            ConnectorNode connectorNode = (ConnectorNode) node;
            connectionFunctions = connectorNode.getConnectorFunctions();
            if (null != connectionFunctions) {
                function = connectionFunctions.getErrorHandleFunction();
            } else {
                throw new CoreException("ConnectionFunctions must be not null,connectionNode does not contain connectionFunctions");
            }
            tapConnectionContext = connectorNode.getConnectorContext();
        }
        if (null == tapConnectionContext) {
            throw new IllegalArgumentException("Auto retry failed, cause tapConnectionContext:[ConnectionContext or ConnectorContext] is null,the param must not be null");
        }
        functionAndContext.errorHandleFunction(function).tapConnectionContext(tapConnectionContext);
    }

    public static void autoRetryAsync(AnyError runnable, String tag, String message, long times, long periodSeconds) {
        try {
            runnable.run();
        } catch(Throwable throwable) {
            TapLogger.info(tag, "AutoRetryAsync info: retry times ({}) | periodSeconds ({}). Please wait...\\n\"", message, times, periodSeconds);
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
            TapLogger.warn(tag, "Error code {} message {} will be ignored. ", coreException.getCode(), ExceptionUtils.getStackTrace(coreException));
        } catch(Throwable throwable) {
            if(!(throwable instanceof QuiteException)) {
                TapLogger.warn(tag, "Unknown error message {} will be ignored. ", ExceptionUtils.getStackTrace(throwable));
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
        handleAnyError(r, null);
    }
    public static void handleAnyError(AnyError r, Consumer<Throwable> consumer) {
        try {
            r.run();
        } catch(CoreException coreException) {
            if(consumer != null) {
                consumer.accept(coreException);
            } else {
                throw coreException;
            }
        } catch(Throwable throwable) {
            if(consumer != null) {
                consumer.accept(throwable);
            } else {
                throw new CoreException(PDKRunnerErrorCodes.COMMON_UNKNOWN, throwable.getMessage(), throwable);
            }
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

    public static String getProperty(String key) {
        String value = System.getProperty(key);
        if(value == null)
            value = System.getenv(key);
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
    public static byte[] encryptWithRC4(byte[] content, String key) throws Exception {
        SecureRandom secureRandom = SecureRandom.getInstance("SHA1PRNG");
        secureRandom.setSeed(key.getBytes());
        KeyGenerator keyGenerator = KeyGenerator.getInstance("RC4");
        keyGenerator.init(secureRandom);
        SecretKey secretKey = keyGenerator.generateKey();

        Cipher cipher = Cipher.getInstance("RC4");
        cipher.init(Cipher.ENCRYPT_MODE, secretKey);

        return cipher.doFinal(content);
    }

    public static byte[] decryptWithRC4(byte[] cipherText, String key) throws Exception {
        SecureRandom secureRandom = SecureRandom.getInstance("SHA1PRNG");
        secureRandom.setSeed(key.getBytes());
        KeyGenerator keyGenerator = KeyGenerator.getInstance("RC4");
        keyGenerator.init(secureRandom);
        SecretKey secretKey = keyGenerator.generateKey();

        Cipher cipher = Cipher.getInstance("RC4");
        cipher.init(Cipher.DECRYPT_MODE, secretKey);

        return cipher.doFinal(cipherText);
    }

    public static int getPdkBuildNumer() {
        AtomicInteger pdkAPIBuildNumber = new AtomicInteger(0);
        ignoreAnyError(() -> {
            InputStream resourceAsStream = CommonUtils.class.getClassLoader().getResourceAsStream("pluginKit.properties");
            Properties properties = new Properties();
            properties.load(resourceAsStream);
            String pdkAPIVersion = properties.getProperty("tapdata.pdk.api.verison");
            pdkAPIBuildNumber.set(getPdkBuildNumer(pdkAPIVersion));
        }, "");
        return pdkAPIBuildNumber.get();
    }

    public static int getPdkBuildNumer(String pdkAPIVersion) {
        AtomicInteger pdkAPIBuildNumber = new AtomicInteger(0);
        ignoreAnyError(() -> {
            Optional.ofNullable(pdkAPIVersion).ifPresent(version -> {
                LinkedList<String> collect = Arrays.stream(pdkAPIVersion.split("[.]")).collect(Collectors.toCollection(LinkedList::new));
                String last = collect.getLast();
                if (collect.size() != 3) {
                    pdkAPIBuildNumber.set(0);
                } else if (last.contains("-SNAPSHOT")) {
                    String temp = StringUtils.replace(last, "-SNAPSHOT", "");
                    if (temp.chars().allMatch(Character::isDigit)) {
                        pdkAPIBuildNumber.set(Integer.parseInt(temp));
                    }
                } else if (last.chars().allMatch(Character::isDigit)) {
                    pdkAPIBuildNumber.set(Integer.parseInt(last));
                }
            });
        }, "");
        return pdkAPIBuildNumber.get();
    }

    public static void main(String[] args) {
//        AtomicLong counter = new AtomicLong();
//
//        int times = 2000000;
//        long time = System.currentTimeMillis();
//        for(int i = 0; i < times; i++) {
//            Runnable r = new Runnable() {
//                @Override
//                public void run() {
//                    counter.incrementAndGet();
//                }
//            };
//            r.run();
//        }
//        System.out.println("1takes " + (System.currentTimeMillis() - time));
//
//        time = System.currentTimeMillis();
//        for(int i = 0; i < times; i++) {
//            Runnable r = () -> counter.incrementAndGet();
//            r.run();
//        }
//        System.out.println("2takes " + (System.currentTimeMillis() - time));
    }

    static class FunctionAndContext{
        ErrorHandleFunction errorHandleFunction = null;
        TapConnectionContext tapConnectionContext = null;
        public static FunctionAndContext create(){ return new FunctionAndContext();}
        public ErrorHandleFunction errorHandleFunction() {
            return errorHandleFunction;
        }

        public FunctionAndContext errorHandleFunction(ErrorHandleFunction function) {
            this.errorHandleFunction = function;
            return this;
        }

        public TapConnectionContext tapConnectionContext() {
            return tapConnectionContext;
        }

        public FunctionAndContext tapConnectionContext(TapConnectionContext tapConnectionContext) {
            this.tapConnectionContext = tapConnectionContext;
            return this;
        }
    }

    public static void countDownAwait(Predicate<Void> stop, CountDownLatch countDownLatch) {
        while (true) {
            if (null != stop && stop.test(null)) {
                break;
            }
            try {
                if (countDownLatch.await(1, TimeUnit.SECONDS)) {
                    break;
                }
            } catch (InterruptedException e) {
                break;
            }
        }
    }
}

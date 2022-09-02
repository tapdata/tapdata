package io.tapdata.pdk.core.utils;

import io.tapdata.entity.error.TapAPIErrorCodes;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.error.CoreException;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.functions.ConnectionFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connection.ErrorHandleFunction;
import io.tapdata.pdk.apis.functions.connection.RetryOptions;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectionNode;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.Node;
import io.tapdata.pdk.core.error.PDKRunnerErrorCodes;
import io.tapdata.pdk.core.error.QuiteException;
import io.tapdata.pdk.core.executor.ExecutorsManager;
import org.apache.commons.io.output.AppendableOutputStream;

import javax.naming.CommunicationException;
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

    public static class AutoRetryParams {
        public void awakenRetryWait() {
            times.set(0);
            synchronized (this) {
                this.notifyAll();
            }
        }

        private PDKMethod method;
        public AutoRetryParams method(PDKMethod method) {
            this.method = method;
            return this;
        }
        private Node node;
        public AutoRetryParams node(Node node) {
            this.node = node;
            return this;
        }
        private AnyError runnable;
        public AutoRetryParams runnable(AnyError runnable) {
            this.runnable = runnable;
            return this;
        }
        private String tag;
        public AutoRetryParams tag(String tag) {
            this.tag = tag;
            return this;
        }
        private String message;
        public AutoRetryParams message(String message) {
            this.message = message;
            return this;
        }
        private AtomicLong times;
        public AutoRetryParams times(AtomicLong times) {
            this.times = times;
            return this;
        }
        private long periodSeconds;
        public AutoRetryParams periodSeconds(long periodSeconds) {
            this.periodSeconds = periodSeconds;
            return this;
        }
        private boolean async;
        public AutoRetryParams async(boolean async) {
            this.async = async;
            return this;
        }

        public AnyError getRunnable() {
            return runnable;
        }

        public void setRunnable(AnyError runnable) {
            this.runnable = runnable;
        }

        public String getTag() {
            return tag;
        }

        public void setTag(String tag) {
            this.tag = tag;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public AtomicLong getTimes() {
            return times;
        }

        public void setTimes(AtomicLong times) {
            this.times = times;
        }

        public long getPeriodSeconds() {
            return periodSeconds;
        }

        public void setPeriodSeconds(long periodSeconds) {
            this.periodSeconds = periodSeconds;
        }

        public boolean isAsync() {
            return async;
        }

        public void setAsync(boolean async) {
            this.async = async;
        }

        public Node getNode() {
            return node;
        }

        public void setNode(Node node) {
            this.node = node;
        }

        public PDKMethod getMethod() {
            return method;
        }

        public void setMethod(PDKMethod method) {
            this.method = method;
        }
    }

    public static void awakenRetryObj(Object syncWaitObj) {
        if(syncWaitObj != null) {
            synchronized (syncWaitObj) {
                syncWaitObj.notifyAll();
            }
        }
    }

    public static void autoRetry(AutoRetryParams autoRetryParams) {
        if(autoRetryParams.periodSeconds <= 0) {
            autoRetryParams.periodSeconds = 10;
//            throw new IllegalArgumentException("periodSeconds can not be zero or less than zero");
        }
        try {
            autoRetryParams.runnable.run();
        } catch(Throwable throwable) {
            Node node = autoRetryParams.node;
            ErrorHandleFunction function = null;
            TapConnectionContext tapConnectionContext = null;
            ConnectionFunctions<?> connectionFunctions = null;
            if(node instanceof ConnectionNode) {
                ConnectionNode connectionNode = (ConnectionNode) node;
                connectionFunctions = connectionNode.getConnectionFunctions();
                if (null != connectionFunctions) {
                    function = connectionFunctions.getErrorHandleFunction();
                }else {
                    throw new CoreException("Connectionfunctions must be not null,connectionNode does not contain Connectionfunctions");
                }
                tapConnectionContext = connectionNode.getConnectionContext();
            } else if(node instanceof ConnectorNode) {
                ConnectorNode connectorNode = (ConnectorNode) node;
                connectionFunctions = connectorNode.getConnectorFunctions();
                if (null != connectionFunctions) {
                    function = connectionFunctions.getErrorHandleFunction();
                }else {
                    throw new CoreException("connectionFunctions must be not null,connectionNode does not contain connectionFunctions");
                }
                tapConnectionContext = connectorNode.getConnectorContext();
            }
            if (null == tapConnectionContext){
                throw new IllegalArgumentException("NeedTry filed ,cause tapConnectionContext:[ConnectionContext or ConnectorContext] is Null,the param must not be null!");
            }

            if(null == function){
                throw new CoreException( "PDK data source not support retry: " + autoRetryParams.tag);
            }

            ErrorHandleFunction finalFunction = function;
            TapConnectionContext finalTapConnectionContext = tapConnectionContext;
            try {
                RetryOptions retryOptions = finalFunction.needRetry(finalTapConnectionContext, autoRetryParams.method, throwable);
                if(retryOptions == null || !retryOptions.isNeedRetry()) {
                    throw throwable;
                }
                if(retryOptions.getBeforeRetryMethod() != null) {
                    CommonUtils.ignoreAnyError(() -> retryOptions.getBeforeRetryMethod().run(), autoRetryParams.tag);
                }
            } catch (Throwable e) {
                e.printStackTrace();
                throw new CoreException(TapAPIErrorCodes.NEED_RETRY_FAILED, "Need retry failed:"+autoRetryParams.tag);
            }

            TapLogger.error(autoRetryParams.tag, "AutoRetryAsync error {}, execute message {}, retry times {}, periodSeconds {}. ", throwable.getMessage(), autoRetryParams.message, autoRetryParams.times, autoRetryParams.periodSeconds);
            if(autoRetryParams.times.get() > 0) {
                autoRetryParams.times.set( autoRetryParams.times.get() - 1 );
                if(autoRetryParams.async) {
                    ExecutorsManager.getInstance().getScheduledExecutorService().schedule(() -> autoRetry(autoRetryParams), autoRetryParams.periodSeconds, TimeUnit.SECONDS);
                } else {
                    synchronized (autoRetryParams) {
                        try {
                            autoRetryParams.wait(autoRetryParams.periodSeconds * 1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    autoRetry(autoRetryParams);
                }
            } else {
                if(throwable instanceof CoreException) {
                    throw (CoreException) throwable;
                }
                throw new CoreException(PDKRunnerErrorCodes.COMMON_UNKNOWN, autoRetryParams.message + " execute failed, " + throwable.getMessage());
            }
        }
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
            TapLogger.warn(tag, "Error code {} message {} will be ignored. ", coreException.getCode(), coreException.getMessage());
        } catch(Throwable throwable) {
            if(!(throwable instanceof QuiteException)) {
                throwable.printStackTrace();
                TapLogger.warn(tag, "Unknown error message {} will be ignored. ", throwable.getMessage());
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

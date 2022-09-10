package io.tapdata.connector.dameng.cdc.logminer.constant;

public class LogmnrDll {

        public static final int LOGMNR_ATTR_PARALLEL_NUM = 10001;

        public static final int LOGMNR_ATTR_BUFFER_NUM = 10002;

        public static final int LOGMNR_ATTR_CONTENT_NUM = 10003;

        static {
            JNIUtil.loadSSLLibrary();
            JNIUtil.loadLibrary(JNIUtil.isWindows() ? "zlib" : "z");
            JNIUtil.loadDmLibrarys("dmdpi");
            JNIUtil.loadDmLibrarys("dmlogmnr_client");
        }

        public static native int initLogmnr();

        public static native long createConnect(String paramString1, int paramInt, String paramString2, String paramString3);

        public static native int addLogFile(long paramLong, String paramString, int paramInt);

        public static native int removeLogFile(long paramLong, String paramString);

        public static native int startLogmnr(long paramLong1, long paramLong2, String paramString1, String paramString2);

        public static native LogmnrRecord[] getData(long paramLong, int paramInt);

        public static native int endLogmnr(long paramLong, int paramInt);

        public static native int closeConnect(long paramLong);

        public static native int deinitLogmnr();

        public static native int setAttr(long paramLong, int paramInt1, int paramInt2);
    }

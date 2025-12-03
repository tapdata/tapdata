package com.tapdata.tm.taskinspect;

import com.tapdata.tm.utils.MD5Utils;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ObjectSerializable;

import java.io.*;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

/**
 * 任务校验工具类
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/1/18 00:58 Create
 */
public interface TaskInspectUtils {
    String MODULE_NAME = "TaskInspect";

    ExecutorService executorService = Executors.newFixedThreadPool(Integer.MAX_VALUE);

    static void close(AutoCloseable... closeableArr) throws Exception {
        AtomicReference<Exception> error = new AtomicReference<>(null);
        for (AutoCloseable c : closeableArr) {
            if (null == c) continue;
            try {
                c.close();
            } catch (Exception e) {
                if (!error.compareAndSet(null, e)) {
                    error.get().addSuppressed(e);
                }
            }
        }
        if (null != error.get()) {
            throw error.get();
        }
    }

    static void stop(long timeout, BooleanSupplier... suppliers) throws InterruptedException {
        boolean hasFalse = true;
        long start = System.currentTimeMillis();
        while (hasFalse) {
            hasFalse = false;
            for (BooleanSupplier s : suppliers) {
                hasFalse = hasFalse || !s.getAsBoolean();
            }

            long times = System.currentTimeMillis() - start;
            if (times > timeout) {
                throw new RuntimeException("Timeout waiting " + times + "ms for " + TaskInspectUtils.MODULE_NAME + " stop");
            }
            TimeUnit.SECONDS.sleep(1);
        }
    }

    static Future<?> submit(Runnable runnable) {
        return executorService.submit(runnable);
    }

    static String toRowId(String tableName, LinkedHashMap<String, Object> keys) {
        StringBuilder buf = new StringBuilder(tableName);
        String serializedKeyStr = keysSerialization(keys);
        buf.append("|").append(serializedKeyStr);
        return MD5Utils.toLowerHex(buf.toString());
    }

    @Deprecated(since = "4.10.0")
    static String encodeKeys(Object obj) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            try (ObjectOutputStream odos = new ObjectOutputStream(baos)) {
                odos.writeObject(obj);
            }
            byte[] bytes = baos.toByteArray();
            return Base64.getEncoder().encodeToString(bytes);
        }
    }

    @Deprecated(since = "4.10.0")
    static <T> T decodeKeys(String keysStr) throws IOException, ClassNotFoundException {
        byte[] bytes = Base64.getDecoder().decode(keysStr);
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
            try (ObjectInputStream ois = new ObjectInputStream(bais)) {
                return (T) ois.readObject();
            }
        }
    }

    /**
     * 主键序列化
     *
     * @param keys 主键数据
     * @return Base64 格式序列化数据
     */
    static String keysSerialization(LinkedHashMap<String, Object> keys) {
        ObjectSerializable serializable = InstanceFactory.instance(ObjectSerializable.class);
        byte[] bytes = serializable.fromObject(keys);
        return Base64.getEncoder().encodeToString(bytes);
    }

    /**
     * 主键反序列化
     *
     * @param serializedData 序列化数据
     * @return 主键数据
     */
    static LinkedHashMap<String, Object> keysDeserialization(String serializedData, ClassLoader classLoader) {
        ObjectSerializable serializable = InstanceFactory.instance(ObjectSerializable.class);
        byte[] bytes = Base64.getDecoder().decode(serializedData);
        Object object = serializable.toObject(bytes, new ObjectSerializable
            .ToObjectOptions()
            .classLoader(classLoader)
            .skipSerialVersionUID(true)
        );

        // 兼容 4.9.0 的数据
        if (null == object) {
            try {
                return decodeKeys(serializedData);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return (LinkedHashMap<String, Object>) object;
    }
}

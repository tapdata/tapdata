package com.tapdata.tm.worker;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/2/15 19:20 Create
 */
public class WorkerSingletonLock implements AutoCloseable {

    private static final boolean ENABLE = true;
    public static final String STOP_AGENT = "To stop agent";
    private static final String FILE_PATH = ".agentSingletonLock";

    private static WorkerSingletonLock instance;
    private final String lastTag;
    private String currentTag;
    private final FileOutputStream fos;
    private final FileChannel channel;
    private final FileLock lock;

    private WorkerSingletonLock(String filePath) throws IOException {
        if (isExistLockFile(filePath)) {
            this.lastTag = loadLockFile(filePath);
        } else {
            this.lastTag = "";
        }
        this.fos = new FileOutputStream(filePath);
        this.channel = fos.getChannel();
        this.lock = channel.lock();
        instance = this;
    }

    private boolean isExistLockFile(String filePath) throws IOException {
        File file = new File(filePath).getAbsoluteFile();
        if (!file.exists()) {
            File parentFile = file.getParentFile();
            if (!parentFile.exists()) {
                parentFile.mkdirs();
            }
            file.createNewFile();
            return false;
        }
        return true;
    }

    private String loadLockFile(String filePath) throws IOException {
        try (FileReader fr = new FileReader(filePath)) {
            StringBuilder buf = new StringBuilder();
            int c;
            while (-1 != (c = fr.read())) {
                buf.append((char) c);
            }
            return buf.toString();
        }
    }

    public void writeLockFile(String tag) throws IOException {
        byte[] tagBytes = tag.getBytes(StandardCharsets.UTF_8);
        channel.truncate(0).write(ByteBuffer.wrap(tagBytes));
        this.currentTag = tag;
    }

    public String getLastTag() {
        return this.lastTag;
    }

    @Override
    public void close() throws Exception {
        try {
            if (null != lock) {
                lock.release();
                lock.close();
            }
        } finally {
            try {
                if (null != channel) {
                    channel.close();
                }
            } finally {
                if (null != fos) fos.close();
            }
        }
    }

    private static synchronized WorkerSingletonLock getInstance(String workDir) throws IOException {
        if (null == instance) {
            StringBuilder sb = new StringBuilder();
            if (StringUtils.isNotBlank(workDir)) {
                sb.append(workDir).append("/").append(FILE_PATH);
            } else {
                sb.append(FILE_PATH);
            }
            instance = new WorkerSingletonLock(sb.toString());
        }
        return instance;
    }

    public static String getCurrentTag() {
        return instance.currentTag;
    }

    public static void check(String workDir, Function<String, String> call) {
        WorkerSingletonLock instance;
        try {
            instance = getInstance(workDir);
        } catch (IOException e) {
            throw new RuntimeException("Init singleton lock failed: " + e.getMessage(), e);
        }

        if (ENABLE) {
            String newTag = call.apply(instance.getLastTag());
            try {
                instance.writeLockFile(newTag);
            } catch (IOException e) {
                throw new RuntimeException("Store singleton lock failed: " + e.getMessage(), e);
            }
        }
    }

    public static String addTag2WsUrl(String url) {
        if (ENABLE) {
            return url + "&singletonTag=" + getCurrentTag();
        }
        return url;
    }

    public static boolean checkDBTag(String tag, String workType, Supplier<String> getDBTag) {
        if (ENABLE && "connector".equals(workType)) {
            String dbTag = getDBTag.get();
            if (tag == null && dbTag == null) {
                return true;
            }
            return null != tag && tag.equals(dbTag);
        }
        return true;
    }
}

package io.tapdata.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * 引擎工具类
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/10/31 16:12 Create
 */
public class EngineHelper {
    private static final Logger LOGGER = LogManager.getLogger(EngineHelper.class);

    private final Charset charset;
    private final String workDir;
    private final VfsHelper vfs;

    // 单例控制
    private EngineHelper(Charset charset, String workDir, VfsHelper vfs) {
        this.charset = charset;
        this.workDir = workDir;
        this.vfs = vfs;
    }

    // ---------- 开放函数 ----------

    public static Charset charset() {
        return ins().charset;
    }

    public static String workDir() {
        return ins().workDir;
    }

    public static VfsHelper vfs() {
        return ins().vfs;
    }

    public static byte[] getBytes(String s) {
        return s.getBytes(charset());
    }

    // ---------- 内部函数 ----------

    private static EngineHelper ins() {
        return Holder.INSTANCE;
    }

    private static String initWorkDir() {
        try {
            // 从环境变量中获取工作目录
            String workDir = System.getenv("TAPDATA_WORK_DIR");
            if (null == workDir) {
                workDir = ".";
            }

            // 初始化工作目录
            Path workDirPath = Paths.get(workDir);
            File workDirFile = workDirPath.toFile();
            if (workDirFile.mkdirs()) {
                LOGGER.info("init work_dir directories: {}", workDirPath);
            }

            // 获取工作目录的绝对路径
            return workDirFile.getCanonicalPath();
        } catch (IOException e) {
            throw new RuntimeException("init work_dir failed", e);
        }
    }

    // 静态内部类懒加载单例，当调用静态方法 ins() 时，才触发静态内部类加载，从而初始化单例（线程安全）
    private static class Holder {
        private static final EngineHelper INSTANCE;
        static {
            Charset charset = StandardCharsets.UTF_8;
            String workDir = initWorkDir();
            INSTANCE = new EngineHelper(charset, workDir, new VfsHelper(workDir, charset));
        }
    }

}

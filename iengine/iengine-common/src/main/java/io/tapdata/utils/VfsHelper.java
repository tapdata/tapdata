package io.tapdata.utils;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * 文件操作工具
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/10/31 16:40 Create
 */
public class VfsHelper {
    private static final Logger LOGGER = LogManager.getLogger(VfsHelper.class);
    private final static OpenOption[] APPEND_OPTIONS = new OpenOption[]{StandardOpenOption.CREATE, StandardOpenOption.APPEND};

    @Getter
    private final Charset charset;
    @Getter
    private final Path homePath;
    @Getter
    private final String home;

    public VfsHelper(String homeStr, Charset charset) {
        this.charset = charset;
        this.home = homeStr;
        this.homePath = Paths.get(homeStr);

        try {
            if (!Files.exists(homePath)) {
                Files.createDirectories(homePath);
            }
        } catch (IOException e) {
            throw new RuntimeException("init vfs_home failed", e);
        }

        LOGGER.info("init vfs_home success, path: {}", homePath);
    }

    /**
     * 文件是否存在
     *
     * @param filepath 文件路径（相对路径）
     * @return 存在返回true
     */
    public boolean exists(String filepath) {
        Path path = homePath.resolve(filepath);
        return Files.exists(path);
    }

    /**
     * 删除文件
     *
     * @param filepath 文件路径（相对路径）
     * @return 删除成功返回true
     * @throws IOException 删除失败
     */
    public boolean delete(String filepath) throws IOException {
        Path path = homePath.resolve(filepath);
        return Files.deleteIfExists(path);
    }

    /**
     * 追加写入，如果文件不存在则创建
     *
     * @param filepath 文件路径（相对路径）
     * @param content  内容
     * @throws IOException 追加失败
     */
    public void append(String filepath, byte[] content) throws IOException {
        Path path = homePath.resolve(filepath);
        try {
            Files.write(path, content, APPEND_OPTIONS);
        } catch (NoSuchFileException e) {
            if (path.toFile().getParentFile().exists()) throw e;

            // 目录不存在时，进行初始化再重试
            Files.createDirectories(path.getParent());
            Files.write(path, content, APPEND_OPTIONS);
        }
    }

    /**
     * 追加写入多行，如果文件不存在则创建
     *
     * @param filepath 文件路径（相对路径）
     * @param lines    行字符串
     * @throws IOException 追加失败
     */
    public void append(String filepath, Iterable<? extends CharSequence> lines) throws IOException {
        Path path = homePath.resolve(filepath);
        try {
            Files.write(path, lines, charset, APPEND_OPTIONS);
        } catch (NoSuchFileException e) {
            if (path.toFile().getParentFile().exists()) throw e;

            // 目录不存在时，进行初始化再重试
            Files.createDirectories(path.getParent());
            Files.write(path, lines, charset, APPEND_OPTIONS);
        }
    }

    /**
     * 删除3天前的文件
     *
     * @param filepath   文件路径（相对路径）
     * @param withParent 是否从父目录进行删除
     * @return 删除的文件数量
     * @throws IOException 删除失败
     */
    public int deleteFrom3DaysAgo(String filepath, boolean withParent) throws IOException {
        long deletionLine = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3); // 3天前的才删除
        Path resolve = homePath.resolve(filepath);
        if (withParent) {
            try (Stream<Path> pathStream = Files.walk(resolve.getParent())) {
                return pathStream.filter(Files::isRegularFile) // 只处理普通文件
                    .map(Path::toFile) // 超过指定时间，删除
                    .mapToInt(file -> deleteOfDeletionLine(file, deletionLine))
                    .sum();
            }
        } else {
            File file = resolve.toFile();
            return deleteOfDeletionLine(file, deletionLine);
        }
    }

    /**
     * 删除指定时间前的文件
     *
     * @param file         文件
     * @param deletionLine 删除时间
     * @return 删除的文件数量
     */
    private int deleteOfDeletionLine(File file, long deletionLine) {
        long lastModified = file.lastModified();
        if (file.exists() && lastModified < deletionLine) {
            LOGGER.info("delete file: {}", file);
            return file.delete() ? 1 : 0;
        }
        return 0;
    }
}

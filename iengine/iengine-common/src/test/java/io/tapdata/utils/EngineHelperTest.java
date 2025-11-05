package io.tapdata.utils;

import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0   Create
 */
class EngineHelperTest {
    private static final Logger LOGGER = LogManager.getLogger(EngineHelperTest.class);

    String vfsHome = "/tmp/vfs_home";
    MockedStatic<CommonUtils> commonUtilsMockedStatic;

    @BeforeEach
    void setUp() {
        commonUtilsMockedStatic = mockStatic(CommonUtils.class);
        commonUtilsMockedStatic.when(()->CommonUtils.getenv("TAPDATA_VFS_HOME")).thenReturn(vfsHome);
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            // 释放测试产生的文件及目录
            Path vfsHomePath = Paths.get(vfsHome);
            try (Stream<Path> pathStream = Files.walk(vfsHomePath)) {
                pathStream.map(Path::toFile)
                    .sorted(Comparator.reverseOrder())
                    .forEach(f -> {
                        if (f.delete()) {
                            LOGGER.info("Remove unitest file: {}", f.getAbsolutePath());
                        } else {
                            LOGGER.info("Remove unitest file failed: {}", f.getAbsolutePath());
                        }
                    });
            }
        } finally {
            commonUtilsMockedStatic.close();
        }
    }

    @Test
    void testMain() throws IOException {
        // 模拟数据
        String testRoot = "unitest"; // 用于测试自动创建目录
        String testFilepath = String.join(File.separator, testRoot, "test.txt");

        VfsHelper vfsHelper = EngineHelper.vfs();

        // 实例不能为空
        assertNotNull(vfsHelper);

        // 测试文件不存在时写入
        vfsHelper.append(testFilepath, "unitest append by bytes".getBytes());
        // 测试文件存在时写入
        vfsHelper.append(testFilepath, List.of("unitest append by lines 1", "unitest append by lines 2"));
        // 测试 exists
        assertTrue(vfsHelper.exists(testFilepath));
        // 测试 delete
        assertTrue(vfsHelper.delete(testFilepath), "delete file failed");
    }
}

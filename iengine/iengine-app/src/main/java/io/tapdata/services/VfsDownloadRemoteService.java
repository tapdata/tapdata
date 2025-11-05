package io.tapdata.services;


import io.tapdata.modules.api.net.data.FileMeta;
import io.tapdata.service.skeleton.annotation.RemoteService;
import io.tapdata.utils.EngineHelper;
import io.tapdata.utils.VfsFilepath;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * RPC 引擎服务 - 文件下载
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/11/4 11:20 Create
 */
@RemoteService
public class VfsDownloadRemoteService {

    /**
     * 下载数据校验修复 SQL
     *
     * @param taskId          任务编号
     * @param inspectResultId 校验结果编号
     * @return 文件元数据
     */
    public FileMeta downloadInspectRecoverSql(String taskId, String inspectResultId) {
        String filepath = VfsFilepath.task_recoverSql_inspect(taskId, inspectResultId);
        String filename = VfsFilepath.task_recoverSql_inspect_downloadName(inspectResultId);
        Path path = EngineHelper.vfs().getHomePath().resolve(filepath);
        return getDownloadFileMeta(path, filename);
    }

    /**
     * 下载增量数据校验修复 SQL
     *
     * @param taskId   任务编号
     * @param manualId 操作编号
     * @return 文件元数据
     */
    public FileMeta downloadTaskInspectRecoverSql(String taskId, String manualId) {
        String filepath = VfsFilepath.task_recoverSql_taskInspect(taskId, manualId);
        String filename = VfsFilepath.task_recoverSql_taskInspect_downloadName(manualId);
        Path path = EngineHelper.vfs().getHomePath().resolve(filepath);
        return getDownloadFileMeta(path, filename);
    }

    /**
     * 获取下载文件元数据
     *
     * @param path     文件路径
     * @param filename 文件名
     * @return 文件元数据
     */
    private FileMeta getDownloadFileMeta(Path path, String filename) {
        if (!Files.exists(path)) {
            return FileMeta.builder().filename(filename).code("FileNotFound").transferFile(false).build();
        }
        try {
            long fileSize = Files.size(path);
            InputStream fileInputStream = Files.newInputStream(path, StandardOpenOption.READ);
            return FileMeta.builder()
                .filename(filename)
                .fileSize(fileSize)
                .code("ok")
                .fileInputStream(fileInputStream)
                .transferFile(true)
                .build();
        } catch (IOException e) {
            return FileMeta.builder().filename(filename).code("ReadFileError").build();
        }
    }
}

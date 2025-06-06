package io.tapdata.services;


import io.tapdata.modules.api.net.data.FileMeta;
import io.tapdata.service.skeleton.annotation.RemoteService;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

@RemoteService
public class ExportEventSqlService {
    private static final String EXPORT_SQL = "exportSql";
    public FileMeta downloadEventSql(String inspectId,String inspectResultId){
        String fileName = inspectResultId + ".sql";
        String file = EXPORT_SQL + File.separator + inspectId + File.separator + fileName;
        Path filePath = Paths.get(file);
        try {
            if (!Files.exists(filePath)) {
                return FileMeta.builder()
                        .filename(fileName)
                        .code("File not exists.")
                        .transferFile(false)
                        .build();
            }
            return FileMeta.builder()
                    .filename(fileName)
                    .fileSize(Files.size(filePath))
                    .code("ok")
                    .fileInputStream(Files.newInputStream(filePath, StandardOpenOption.READ))
                    .transferFile(true)
                    .build();
        } catch (Exception e) {
            return FileMeta.builder().filename(fileName).code("ReadFileError").build();
        }
    }

    public void deleteEventSql(String inspectId,String inspectResultId) throws IOException {
        if(StringUtils.isNotBlank(inspectResultId)){
            String filePath = EXPORT_SQL + File.separator + inspectId + File.separator + inspectResultId + ".sql";
            Files.deleteIfExists(Paths.get(filePath));
        }else{
            String filePath = EXPORT_SQL + File.separator + inspectId;
            try (var stream = Files.list(Paths.get(filePath))) {
                stream
                        .filter(Files::isRegularFile) // 只处理普通文件
                        .forEach(path -> {
                            try {
                                Files.deleteIfExists(path);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
            } catch (IOException e) {
                throw new RuntimeException("Delete file failed: " + e.getMessage());
            }
            Files.deleteIfExists(Paths.get(filePath));
        }



    }
}

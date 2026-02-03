package com.tapdata.tm.group.service.transfer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.group.service.GroupInfoService;
import com.tapdata.tm.task.bean.TaskUpAndLoadDto;
import jakarta.servlet.http.HttpServletResponse;
import lombok.Setter;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.IOUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 文件传输策略
 * 实现基于文件（tar 包）的分组导入导出
 */
@Component
public class FileGroupTransferStrategy implements GroupTransferStrategy {
    @Setter(onMethod_ = {@Autowired, @Lazy})
    private GroupInfoService groupInfoService;

    @Override
    public GroupTransferType getType() {
        return GroupTransferType.FILE;
    }

    @Override
    public void exportGroups(GroupExportRequest request) {
        HttpServletResponse response = request.getResponse();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        String fileName = request.getName() + ".tar";
        try (TarArchiveOutputStream taos = new TarArchiveOutputStream(baos)) {
            addContentToTar(taos, request.getContents());
            taos.finish();
        }catch (Exception e) {
            throw new BizException("Group.Export.Error");
        }
        try (OutputStream out = response.getOutputStream()) {
            response.setHeader("Content-Disposition",
                    "attachment; filename=\"" + fileName + "\"");
            response.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE);
            response.setContentLength(baos.size());
            out.write(baos.toByteArray());
            out.flush();
        }catch (Exception e) {
            throw new BizException("Group.Export.Error");
        }
    }

    @Override
    public ObjectId importGroups(GroupImportRequest request) throws IOException {
        if(request.getResource() == null) {
            throw new BizException("Group.Import.Resource.Null");
        }
        if(!(request.getResource() instanceof MultipartFile file)) {
            throw new BizException("Group.Import.Resource.Type.Error");
        }
        String fileName = file.getOriginalFilename();
        Map<String, List<TaskUpAndLoadDto>> payloads = readGroupImportPayloads(file);
        return groupInfoService.batchImportGroupInternal(
                payloads,
                request.getUser(),
                request.getImportMode(),
                fileName
        );
    }

    protected Map<String, List<TaskUpAndLoadDto>> readGroupImportPayloads(MultipartFile file) throws IOException {
        Map<String, List<TaskUpAndLoadDto>> payloads = new HashMap<>();
        try (TarArchiveInputStream tais = new TarArchiveInputStream(file.getInputStream())) {
            TarArchiveEntry entry;
            while ((entry = tais.getNextTarEntry()) != null) {
                if (entry.isDirectory()) {
                    continue;
                }
                ByteArrayOutputStream entryBuffer = new ByteArrayOutputStream();
                IOUtils.copy(tais, entryBuffer);
                byte[] bytes = entryBuffer.toByteArray();
                String entryName = entry.getName();

                // 判断是否为Excel文件
                if (isExcelFile(entryName)) {
                    // Excel文件作为二进制数据存储
                    List<TaskUpAndLoadDto> list = new ArrayList<>();
                    list.add(new TaskUpAndLoadDto(entryName, bytes));
                    payloads.put(entryName, list);
                } else if(entryName.endsWith(".json")) {
                    // JSON文件按原逻辑解析
                    List<TaskUpAndLoadDto> list = parseTaskUpAndLoadList(bytes);
                    payloads.put(entryName, list);
                }
            }
        }
        return payloads;
    }

    /**
     * 判断是否为Excel文件
     */
    protected boolean isExcelFile(String fileName) {
        if (StringUtils.isBlank(fileName)) {
            return false;
        }
        String lowerName = fileName.toLowerCase();
        return lowerName.endsWith(".xlsx") || lowerName.endsWith(".xls");
    }

    protected List<TaskUpAndLoadDto> parseTaskUpAndLoadList(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return new ArrayList<>();
        }
        String json = new String(bytes, StandardCharsets.UTF_8);
        if (StringUtils.isBlank(json)) {
            return new ArrayList<>();
        }
        List<TaskUpAndLoadDto> list = JsonUtil.parseJsonUseJackson(json, new TypeReference<List<TaskUpAndLoadDto>>() {
        });
        return list == null ? new ArrayList<>() : list;
    }

    protected void addContentToTar(TarArchiveOutputStream taos, Map<String, byte[]> contents) throws IOException {
        for (Map.Entry<String, byte[]> entry : contents.entrySet()) {
            byte[] contentBytes = entry.getValue();
            TarArchiveEntry tarEntry = new TarArchiveEntry(entry.getKey());
            tarEntry.setSize(contentBytes.length);
            taos.putArchiveEntry(tarEntry);
            try (ByteArrayInputStream bais = new ByteArrayInputStream(contentBytes)) {
                IOUtils.copy(bais, taos);
            }
            taos.closeArchiveEntry();
        }
    }
}

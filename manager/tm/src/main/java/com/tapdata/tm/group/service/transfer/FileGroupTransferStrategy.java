package com.tapdata.tm.group.service.transfer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.group.constant.GroupConstants;
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
    public Map<String, List<TaskUpAndLoadDto>> parseImportPayloads(Object resource) throws IOException {
        if (!(resource instanceof MultipartFile file)) {
            throw new BizException("Group.Import.Resource.Type.Error");
        }
        return readGroupImportPayloads(file);
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
        String originalFilename = file.getOriginalFilename();
        if (originalFilename != null && originalFilename.toLowerCase().endsWith(".json")) {
            return readFromJson(file);
        }
        return readFromTar(file);
    }

    private Map<String, List<TaskUpAndLoadDto>> readFromJson(MultipartFile file) throws IOException {
        Map<String, List<TaskUpAndLoadDto>> payloads = new HashMap<>();
        byte[] bytes = file.getBytes();
        List<TaskUpAndLoadDto> list = parseTaskUpAndLoadList(bytes);
        String entryName = file.getOriginalFilename();
        String mappedKey = mapToPayloadKey(entryName != null ? entryName : "");
        payloads.computeIfAbsent(mappedKey, k -> new ArrayList<>()).addAll(list);
        return payloads;
    }

    private Map<String, List<TaskUpAndLoadDto>> readFromTar(MultipartFile file) throws IOException {
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
                } else if (entryName.endsWith(".json")) {
                    String baseName = entryName.contains("/")
                            ? entryName.substring(entryName.lastIndexOf('/') + 1) : entryName;
                    if (GroupConstants.VAULT_FILE.equalsIgnoreCase(baseName)) {
                        // vault.json 是 Map<String,String> 格式，直接保存原始 JSON，不按 TaskUpAndLoadDto 解析
                        List<TaskUpAndLoadDto> list = new ArrayList<>();
                        list.add(new TaskUpAndLoadDto(GroupConstants.VAULT_FILE,
                                new String(bytes, StandardCharsets.UTF_8)));
                        payloads.put(GroupConstants.VAULT_FILE, list);
                    } else {
                        // JSON文件：按文件名映射到对应的资源类型 key，支持新旧格式
                        List<TaskUpAndLoadDto> list = parseTaskUpAndLoadList(bytes);
                        String mappedKey = mapToPayloadKey(entryName);
                        payloads.computeIfAbsent(mappedKey, k -> new ArrayList<>()).addAll(list);
                    }
                }
            }
        }
        return payloads;
    }

    /**
     * 将导入的文件名映射到内部资源类型 key。
     * 兼容新格式（{projectName}_xxx.json）和旧格式（xxx.json）。
     */
    protected String mapToPayloadKey(String entryName) {
        if (entryName.endsWith("_Connection_Config.json") || entryName.endsWith("_Connection_Metadata.json")) {
            return "Connection.json";
        }
        if (entryName.endsWith("_MigrateTask.json")) return "MigrateTask.json";
        if (entryName.endsWith("_SyncTask.json"))    return "SyncTask.json";
        if (entryName.endsWith("_ValidateTask.json")) return "InspectTask.json";
        if (entryName.endsWith("_Module.json"))         return "Module.json";
        if (entryName.endsWith("_ShareCache.json"))     return "ShareCache.json";
        // 兼容 MetadataDefinition.json 位于根目录（旧格式）或 API/ 子目录（新格式）
        if (entryName.endsWith("MetadataDefinition.json")) return "MetadataDefinition.json";
        // GroupInfo.json 以及其他旧格式文件使用 baseName（兼容 tar 中含路径前缀的情况）
        return entryName.contains("/")
                ? entryName.substring(entryName.lastIndexOf('/') + 1) : entryName;
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
        String jsonStr = new String(bytes, StandardCharsets.UTF_8);
        if (StringUtils.isBlank(jsonStr)) {
            return new ArrayList<>();
        }
        // 兼容两种格式：
        // 旧格式：json 字段是转义的 JSON 字符串
        // 新格式（pretty-print 导出）：json 字段是嵌套对象，Jackson 无法直接反序列化为 String
        // 统一先解析为 List<Map>，再手动构建 TaskUpAndLoadDto
        try {
            List<Map<String, Object>> rawList = JsonUtil.parseJsonUseJackson(
                    jsonStr, new TypeReference<List<Map<String, Object>>>() {});
            if (rawList == null) {
                return new ArrayList<>();
            }
            List<TaskUpAndLoadDto> result = new ArrayList<>();
            for (Map<String, Object> raw : rawList) {
                String collectionName = (String) raw.get("collectionName");
                Object jsonValue = raw.get("json");
                String itemJson;
                if (jsonValue instanceof String) {
                    itemJson = (String) jsonValue;
                } else if (jsonValue != null) {
                    // 新格式：json 字段是嵌套对象，序列化回字符串
                    itemJson = JsonUtil.toJsonUseJackson(jsonValue);
                } else {
                    itemJson = null;
                }
                result.add(new TaskUpAndLoadDto(collectionName, itemJson));
            }
            return result;
        } catch (Exception e) {
            return new ArrayList<>();
        }
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

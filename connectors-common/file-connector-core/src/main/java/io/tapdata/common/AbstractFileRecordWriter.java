package io.tapdata.common;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.file.TapFileStorageBuilder;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.storage.local.LocalFileStorage;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public abstract class AbstractFileRecordWriter {

    protected TapFileStorage storage;
    protected TapFileStorage localStorage;
    protected FileConfig fileConfig;
    protected TapTable tapTable;
    protected KVMap<Object> kvMap;
    protected List<String> fieldList;
    protected Map<String, AbstractFileWriter> fileWriterMap;
    protected String writeDateString;
    protected Map<String, Long> lastWriteMap;
    protected String connectorId;

    public AbstractFileRecordWriter(TapFileStorage storage, FileConfig fileConfig, TapTable tapTable, KVMap<Object> kvMap) throws Exception {
        fileWriterMap = new ConcurrentHashMap<>();
        this.storage = storage;
        this.localStorage = new TapFileStorageBuilder()
                .withClassLoader(LocalFileStorage.class.getClassLoader())
                .withStorageClassName("io.tapdata.storage.local.LocalFileStorage")
                .build();
        this.fileConfig = fileConfig;
        this.tapTable = tapTable;
        this.kvMap = kvMap;
        this.fieldList = tapTable.getNameFieldMap().entrySet().stream().sorted(Comparator.comparing(v ->
                EmptyKit.isNull(v.getValue().getPos()) ? 99999 : v.getValue().getPos())).map(Map.Entry::getKey).collect(Collectors.toList());
        lastWriteMap = (Map<String, Long>) kvMap.get("tapdata_file_last_write");
        if (EmptyKit.isNull(lastWriteMap)) {
            lastWriteMap = new ConcurrentHashMap<>();
        }
    }

    public void setConnectorId(String connectorId) {
        this.connectorId = connectorId;
    }

    protected abstract void writeOneFile(List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer, String uniquePath) throws Exception;

    protected abstract void writeMultiFiles(List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer, String fileNameExpression) throws Exception;

    public abstract void write(List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Exception;

    public void mergeCacheFiles() throws Exception {
        for (Map.Entry<String, AbstractFileWriter> entry : fileWriterMap.entrySet()) {
            String uniquePath = entry.getKey();
            String dirPath = uniquePath.substring(0, uniquePath.lastIndexOf("/"));
            List<TapFile> cacheFiles = new ArrayList<>();
            storage.getFilesInDirectory(dirPath, Collections.singletonList(uniquePath + ".tapCache*"), null, false, 5, cacheFiles::addAll);
            if (EmptyKit.isEmpty(cacheFiles)) {
                return;
            }
            String fileName = uniquePath.substring(uniquePath.lastIndexOf("/") + 1);
            String coreLocalFilePath = "cacheFiles" + File.separator + connectorId + File.separator + fileName;
            storage.readFile(uniquePath, inputStream -> {
                File cacheDir = new File("cacheFiles" + File.separator + connectorId);
                if (!cacheDir.exists()) {
                    cacheDir.mkdirs();
                }
                try {
                    localStorage.saveFile(coreLocalFilePath, inputStream, true);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            writeCacheFile(coreLocalFilePath, cacheFiles);
            localStorage.readFile(coreLocalFilePath, inputStream -> {
                try {
                    storage.saveFile(uniquePath + ".tmp", inputStream, true);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            for (TapFile file : cacheFiles) {
                storage.delete(file.getPath());
            }
            storage.move(uniquePath + ".tmp", uniquePath);
        }
    }

    protected abstract void writeCacheFile(String coreLocalFilePath, List<TapFile> cacheFilesPath) throws Exception;

    protected String correctPath(String path) {
        return path.endsWith("/") ? path : (path + "/");
    }

    protected String replaceDateSign(String fileNameExpression) {
        StringBuilder res = new StringBuilder();
        Date date = new Date();
        String subStr = fileNameExpression;
        while (subStr.contains("${date:")) {
            res.append(subStr, 0, subStr.indexOf("${date:"));
            subStr = subStr.substring(subStr.indexOf("${date:") + 7);
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(subStr.substring(0, subStr.indexOf("}")));
            res.append(simpleDateFormat.format(date));
            subStr = subStr.substring(subStr.indexOf("}") + 1);
        }
        res.append(subStr);
        return res.toString();
    }

    protected List<String> getKeyFieldList(List<String> fieldList) {
        List<String> keyFieldList = new ArrayList<>();
        String expression = fileConfig.getFileNameExpression();
        if (EmptyKit.isBlank(expression)) {
            return keyFieldList;
        }
        for (String field : fieldList) {
            if (expression.contains("${record." + field + "}")) {
                keyFieldList.add(field);
            }
        }
        return keyFieldList;
    }

    protected String getFileNameFromValue(String expression, Map<String, Object> value) {
        String key = expression;
        for (String field : fieldList) {
            key = key.replaceAll("\\$\\{record." + field + "}", String.valueOf(value.get(field)));
        }
        return key;
    }

    public void releaseResource() throws Exception {
        fileWriterMap.forEach((k, v) -> v.close());
        localStorage.destroy();
    }
}

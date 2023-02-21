package io.tapdata.storage.oss;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.comm.Protocol;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectMetadata;
import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.storage.kit.EmptyKit;
import io.tapdata.storage.kit.FileMatchKit;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Author:Skeet
 * Date: 2023/1/3
 **/
public class OssFileStorage implements TapFileStorage {

    private final static String TAG = OssFileStorage.class.getSimpleName();
    private OssConfig ossConfig;
    private OSS ossClient;

    @Override
    public void init(Map<String, Object> params) {
        ossConfig = new OssConfig().load(params);
        ClientBuilderConfiguration conf = new ClientBuilderConfiguration();
        conf.setProtocol(Protocol.HTTP);
        ossClient = new OSSClientBuilder().build(ossConfig.getEndpoint(), ossConfig.getAccessKey(), ossConfig.getSecretKey(), conf);
    }

    @Override
    public void destroy() {
        if (EmptyKit.isNotNull(ossClient)) {
            ossClient.shutdown();
        }
    }

    @Override
    public TapFile getFile(String path) throws Exception {
        try {
            OSSObject ossObject = ossClient.getObject(ossConfig.getBucket(), path);
            TapFile tapFile = new TapFile();
            tapFile.type(TapFile.TYPE_FILE)
                    .name(path.substring(path.lastIndexOf("/") + 1))
                    .path(path)
                    .length(ossObject.getObjectMetadata().getContentLength())
                    .lastModified(ossObject.getObjectMetadata().getLastModified().getTime());
            return tapFile;
        } catch (OSSException e) {
            if (isDirectoryExist(path)) {
                TapFile tapFile = new TapFile();
                tapFile.type(TapFile.TYPE_DIRECTORY)
                        .name(path.substring(path.lastIndexOf("/") + 1))
                        .path(path)
                        .length(0L)
                        .lastModified(0L);
                return tapFile;
            } else {
                return null;
            }
        }
    }

    @Override
    public void readFile(String path, Consumer<InputStream> consumer) throws Exception {
        if (!isFileExist(path)) {
            return;
        }
        try (
                InputStream is = ossClient.getObject(ossConfig.getBucket(), path).getObjectContent()
        ) {
            consumer.accept(is);
        }
    }

    @Override
    public boolean isFileExist(String path) {
        try {
            ossClient.doesObjectExist(ossConfig.getBucket(), path);
            return true;
        } catch (OSSException e) {
            return false;
        }
    }

    @Override
    public boolean move(String sourcePath, String destPath) {
        try {
            ossClient.copyObject(ossConfig.getBucket(), sourcePath, ossConfig.getBucket(), destPath);
            ossClient.deleteObject(ossConfig.getBucket(), sourcePath);
            return true;
        } catch (OSSException e) {
            return false;
        }
    }

    @Override
    public boolean delete(String path) throws Exception {

        List<String> deletedObjects = ossClient.deleteObjects(new DeleteObjectsRequest(ossConfig.getBucket())
                .withKeys((List<String>) ossClient.listObjects(ossConfig.getBucket(), path))
                .withEncodingType("url")).getDeletedObjects();
        try {
            for (String obj : deletedObjects) {
                URLDecoder.decode(obj, "UTF-8");
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public TapFile saveFile(String path, InputStream is, boolean canReplace) throws Exception {
        if (!isFileExist(path) || canReplace) {
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentLength(is.available());
            ossClient.putObject(ossConfig.getBucket(), path, is, objectMetadata);
        }
        return getFile(path);
    }

    @Override
    public OutputStream openFileOutputStream(String path, boolean append) throws Exception {
        return null;
    }

    @Override
    public boolean supportAppendData() {
        return TapFileStorage.super.supportAppendData();
    }

    @Override
    public void getFilesInDirectory(String directoryPath, Collection<String> includeRegs,
                                    Collection<String> excludeRegs, boolean recursive,
                                    int batchSize, Consumer<List<TapFile>> consumer) throws Exception {
        if (!isDirectoryExist(directoryPath)) {
            return;
        }
        String newPath = completionPath(directoryPath);
        AtomicReference<List<TapFile>> listAtomicReference = new AtomicReference<>(new ArrayList<>());
        ossClient.listObjectsV2(ossConfig.getBucket(), newPath).getObjectSummaries().forEach(v -> {
            String path = v.getKey();
            String fileName = path.substring(path.lastIndexOf("/") + 1);
            if (recursive || completionPath(newPath).length() + fileName.length() == path.length()) {
                if (FileMatchKit.matchRegs(fileName, includeRegs, excludeRegs)) {
                    listAtomicReference.get().add(toTapFile(v));
                    if (listAtomicReference.get().size() >= batchSize) {
                        consumer.accept(listAtomicReference.get());
                        listAtomicReference.set(new ArrayList<>());
                    }
                }
            }
        });
        if (listAtomicReference.get().size() > 0) {
            consumer.accept(listAtomicReference.get());
        }

    }

    @Override
    public boolean isDirectoryExist(String path) throws Exception {
        return ossClient.listObjectsV2(ossConfig.getBucket(), completionPath(path)).getKeyCount() > 0;
    }

    @Override
    public String getConnectInfo() {
        return "oss://" + ossConfig.getEndpoint() + "/" + ossConfig.getBucket();
    }

    private String completionPath(String path) {
        if (EmptyKit.isBlank(path) || path.equals("/")) {
            return "";
        }
        return path.endsWith("/") ? path : path + "/";
    }

    private TapFile toTapFile(OSSObjectSummary ossObjectSummary) {
        TapFile tapFile = new TapFile();
        String path = ossObjectSummary.getKey();
        tapFile.type(TapFile.TYPE_FILE)
                .name(path.substring(path.lastIndexOf("/") + 1))
                .path(path)
                .length(ossObjectSummary.getSize())
                .lastModified(ossObjectSummary.getLastModified().getTime());
        return tapFile;
    }
}

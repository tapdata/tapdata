package io.tapdata.storege.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.DeleteObjectsRequest;
import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.storage.kit.EmptyKit;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Author:Skeet
 * Date: 2023/1/3
 **/
public class OssFileStorage implements TapFileStorage {
    private OssConfig ossConfig;
    private OSS ossClient;

    @Override
    public void init(Map<String, Object> params) {
        ossConfig = new OssConfig().load(params);
        ossClient = new OSSClientBuilder().build(ossConfig.getEndpoint(), ossConfig.getAccessKeyId(), ossConfig.getAccessKeySecret());
    }

    @Override
    public void destroy() {
        if (EmptyKit.isNotNull(ossClient)) {
            ossClient.shutdown();
        }
    }

    @Override
    public TapFile getFile(String path) throws Exception {
        return null;
    }

    @Override
    public void readFile(String path, Consumer<InputStream> consumer) throws Exception {

    }

    @Override
    public boolean isFileExist(String path) {
        try {
            ossClient.doesObjectExist(ossConfig.getBucketName(), path);
            return true;
        } catch (OSSException e) {
            return false;
        }
    }

    @Override
    public boolean move(String sourcePath, String destPath) {
        try {
            ossClient.copyObject(ossConfig.getBucketName(), sourcePath, ossConfig.getBucketName(), destPath);
            ossClient.deleteObject(ossConfig.getBucketName(), sourcePath);
            return true;
        } catch (OSSException e) {
            return false;
        }
    }

    @Override
    public boolean delete(String path) throws Exception {

        List<String> deletedObjects = ossClient.deleteObjects(new DeleteObjectsRequest(ossConfig.getBucketName())
                .withKeys((List<String>) ossClient.listObjects(ossConfig.getBucketName(), path))
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
        return null;
    }

    @Override
    public void getFilesInDirectory(String directoryPath, Collection<String> includeRegs, Collection<String> excludeRegs, boolean recursive, int batchSize, Consumer<List<TapFile>> consumer) throws Exception {

    }

    @Override
    public boolean isDirectoryExist(String path) throws Exception {
        return false;
    }

    @Override
    public String getConnectInfo() {
        return null;
    }
}

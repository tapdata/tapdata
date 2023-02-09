package io.tapdata.storage.s3fs;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.storage.kit.EmptyKit;
import io.tapdata.storage.kit.FileMatchKit;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class S3fsFileStorage implements TapFileStorage {

    private S3fsConfig s3fsConfig;
    private AmazonS3 amazonS3Client;

    @Override
    public void init(Map<String, Object> params) {
        s3fsConfig = new S3fsConfig().load(params);
        AWSCredentials credentials = new BasicAWSCredentials(s3fsConfig.getAccessKey(), s3fsConfig.getSecretKey());
        amazonS3Client = AmazonS3ClientBuilder.standard()
                .withClientConfiguration(new ClientConfiguration().withProtocol(Protocol.HTTP))
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(s3fsConfig.getEndpoint(), s3fsConfig.getRegion()))
                .build();
    }

    @Override
    public void destroy() {
        if (EmptyKit.isNotNull(amazonS3Client)) {
            amazonS3Client.shutdown();
        }
    }

    @Override
    public TapFile getFile(String path) {
        try {
            S3Object s3Object = amazonS3Client.getObject(s3fsConfig.getBucket(), path);
            TapFile tapFile = new TapFile();
            tapFile.type(TapFile.TYPE_FILE)
                    .name(path.substring(path.lastIndexOf("/") + 1))
                    .path(path)
                    .length(s3Object.getObjectMetadata().getInstanceLength())
                    .lastModified(s3Object.getObjectMetadata().getLastModified().getTime());
            return tapFile;
        } catch (AmazonS3Exception e) {
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

    private TapFile toTapFile(S3ObjectSummary s3ObjectSummary) {
        TapFile tapFile = new TapFile();
        String path = s3ObjectSummary.getKey();
        tapFile.type(TapFile.TYPE_FILE)
                .name(path.substring(path.lastIndexOf("/") + 1))
                .path(path)
                .length(s3ObjectSummary.getSize())
                .lastModified(s3ObjectSummary.getLastModified().getTime());
        return tapFile;
    }

    @Override
    public void readFile(String path, Consumer<InputStream> consumer) throws IOException {
        if (!isFileExist(path)) {
            return;
        }
        try (
                InputStream is = amazonS3Client.getObject(s3fsConfig.getBucket(), path).getObjectContent()
        ) {
            consumer.accept(is);
        }
    }

    @Override
    public boolean isFileExist(String path) {
        try {
            amazonS3Client.getObject(s3fsConfig.getBucket(), path);
            return true;
        } catch (AmazonS3Exception e) {
            return false;
        }
    }

    @Override
    public boolean move(String sourcePath, String destPath) {
        try {
            amazonS3Client.copyObject(s3fsConfig.getBucket(), sourcePath, s3fsConfig.getBucket(), destPath);
            amazonS3Client.deleteObject(s3fsConfig.getBucket(), sourcePath);
            return true;
        } catch (AmazonS3Exception e) {
            return false;
        }
    }

    @Override
    public boolean delete(String path) {
        try {
            amazonS3Client.deleteObjects(new DeleteObjectsRequest(s3fsConfig.getBucket())
                    .withKeys(amazonS3Client.listObjectsV2(s3fsConfig.getBucket(), path).getObjectSummaries()
                            .stream().map(v -> new DeleteObjectsRequest.KeyVersion(v.getKey())).collect(Collectors.toList())));
            return true;
        } catch (AmazonS3Exception e) {
            return false;
        }
    }

    @Override
    public TapFile saveFile(String path, InputStream is, boolean canReplace) throws IOException {
        if (!isFileExist(path) || canReplace) {
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentLength(is.available());
            amazonS3Client.putObject(s3fsConfig.getBucket(), path, is, objectMetadata);
        }
        return getFile(path);
    }

    @Override
    public OutputStream openFileOutputStream(String path, boolean append) {
        return new S3OutputStream(amazonS3Client, s3fsConfig.getBucket(), path);
    }

    @Override
    public boolean supportAppendData() {
        return false;
    }

    @Override
    public void getFilesInDirectory(String directoryPath, Collection<String> includeRegs, Collection<String> excludeRegs, boolean recursive, int batchSize, Consumer<List<TapFile>> consumer) {
        if (!isDirectoryExist(directoryPath)) {
            return;
        }
        AtomicReference<List<TapFile>> listAtomicReference = new AtomicReference<>(new ArrayList<>());
        amazonS3Client.listObjectsV2(s3fsConfig.getBucket(), directoryPath).getObjectSummaries().forEach(v -> {
            String path = v.getKey();
            String fileName = path.substring(path.lastIndexOf("/") + 1);
            if (recursive || (completionPath(directoryPath).length() + fileName.length() == path.length())) {
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
    public boolean isDirectoryExist(String path) {
        return amazonS3Client.listObjectsV2(s3fsConfig.getBucket(), completionPath(path)).getKeyCount() > 0;
    }

    @Override
    public String getConnectInfo() {
        return "s3fs://" + s3fsConfig.getEndpoint() + "/" + s3fsConfig.getBucket() + "/";
    }

    private String completionPath(String path) {
        if (EmptyKit.isBlank(path) || path.equals("/")) {
            return "";
        }
        return path.endsWith("/") ? path : path + "/";
    }
}

package tests;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.util.StringUtils;
import io.tapdata.file.TapFileStorage;
import io.tapdata.file.TapFileStorageBuilder;
import io.tapdata.storage.s3fs.S3fsFileStorage;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;

public class Main {

    @Test
    public void test() throws Exception {
//        String accessKey = "k1nBAv1kKJwGRDJv";
//        String secretKey = "1d3Ip6HWTIOKMeQLyGDwvQsaN2TZV3A0";
//        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
//
//        AmazonS3 conn = AmazonS3ClientBuilder.standard()
//                .withClientConfiguration(new ClientConfiguration().withProtocol(Protocol.HTTP))
//                .withCredentials(new AWSStaticCredentialsProvider(credentials))
//                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("localhost:9000", null))
//                .build();
//        List<Bucket> buckets = conn.listBuckets();
//        for (Bucket bucket : buckets) {
//            System.out.println(bucket.getName() + "\t" +
//                    StringUtils.fromDate(bucket.getCreationDate()));
//        }
//        conn.shutdown();
        TapFileStorage storage = new TapFileStorageBuilder()
                .withClassLoader(S3fsFileStorage.class.getClassLoader()) //PDK's classloader
                .withParams(map(
                        entry("endpoint", "127.0.0.1:9000"),
                        entry("accessKey", "k1nBAv1kKJwGRDJv"),
                        entry("secretKey", "1d3Ip6HWTIOKMeQLyGDwvQsaN2TZV3A0"),
                        entry("bucket", "jarad")
                ))
                .withStorageClassName("io.tapdata.storage.s3fs.S3fsFileStorage")
                .build();
//        InputStream is = storage.readFile("COOLGJ/credentials.json");
//        storage.saveFile("耿杰/aaa.json", is, true);
//        is.close();
//        System.out.println(storage.isDirectoryExist("/"));
        storage.getFilesInDirectory("", Collections.singletonList("*.vbs"), null, true, 1, System.out::println);
        storage.destroy();
    }
}

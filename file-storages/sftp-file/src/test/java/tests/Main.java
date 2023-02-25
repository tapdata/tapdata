package tests;

import io.tapdata.file.TapFileStorage;
import io.tapdata.file.TapFileStorageBuilder;
import io.tapdata.storage.sftp.SftpFileStorage;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Collections;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;

public class Main {
    @Test
    public void test() throws Exception {
        TapFileStorage storage = new TapFileStorageBuilder()
                .withClassLoader(SftpFileStorage.class.getClassLoader()) //PDK's classloader
                .withParams(map(
                        entry("sftpHost", "192.168.1.189"),
                        entry("sftpPort", 22),
                        entry("sftpUsername", "root"),
                        entry("sftpPassword", "Gotapd8!"),
                        entry("encoding", "UTF-8")
                ))
                .withStorageClassName("io.tapdata.storage.sftp.SftpFileStorage")
                .build();
//        InputStream is = storage.readFile("/root/j/jmc.txt");
//        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
//        int len;
//        byte[] bytes = new byte[1024];
//        while ((len = is.read(bytes)) != -1) {
//            byteArrayOutputStream.write(bytes, 0, len);
//        }
//        is.close();
//        InputStream nis = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
//        storage.saveFile("/root/jarad/ks.txt", nis, true);
//        byteArrayOutputStream.close();
//        nis.close();
//        System.out.println(storage.getFile("/root/j/jmc.txt"));
        storage.getFilesInDirectory("/root", Collections.singleton("*.txt"), null, false, 1, System.out::println);
        storage.destroy();
    }
}

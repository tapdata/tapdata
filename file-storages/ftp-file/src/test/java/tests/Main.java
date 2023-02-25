package tests;

import io.tapdata.file.TapFileStorage;
import io.tapdata.file.TapFileStorageBuilder;
import io.tapdata.storage.ftp.FtpFileStorage;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;

public class Main {

    @Test
    public void test() throws Exception {
        TapFileStorage storage = new TapFileStorageBuilder()
                .withClassLoader(FtpFileStorage.class.getClassLoader()) //PDK's classloader
				.withParams(map(
                        entry("ftpHost", "192.168.1.126"),
                        entry("ftpPort", 50021),
                        entry("ftpUsername", "root"),
                        entry("ftpPassword", "Gotapd8!"),
                        entry("encoding", "UTF-8")
                ))
                .withStorageClassName("io.tapdata.storage.ftp.FtpFileStorage")
                .build();
//        InputStream is = storage.readFile("/耿杰/大男孩");
//        storage.saveFile("/fuck", is, true);
//        is.close();
//        storage.isDirectoryExist("/fuck");
        storage.getFilesInDirectory("/", Collections.singletonList("*.xlsx"), Collections.singletonList("*1.xlsx"), true, 10, System.out::println);
        storage.destroy();
    }

}

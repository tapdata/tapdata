package tests;

import io.tapdata.file.TapFileStorage;
import io.tapdata.file.TapFileStorageBuilder;
import io.tapdata.storage.smb.SmbFileStorage;
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
                .withClassLoader(SmbFileStorage.class.getClassLoader()) //PDK's classloader
				.withParams(map(
                        entry("smbHost", "192.168.0.110"),
                        entry("smbShareDir", "share")
                ))
                .withStorageClassName("io.tapdata.storage.smb.SmbFileStorage")
                .build();
//        InputStream is = storage.readFile("jarad\\gj.xls");
//        storage.saveFile("耿杰\\gjgj.xls", is, true);
//        is.close();
        storage.getFilesInDirectory("", Collections.singleton("*.txt"), Collections.singleton("*e.txt"), true, 1, System.out::println);
        storage.destroy();
    }
}

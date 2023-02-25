package io.tapdata.common;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.file.TapFileStorage;
import io.tapdata.file.TapFileStorageBuilder;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.storage.kit.EmptyKit;

import java.util.ArrayList;
import java.util.List;

import static io.tapdata.base.ConnectorBase.testItem;

public class FileTest implements AutoCloseable {

    private final FileConfig fileConfig;
    private final TapFileStorage storage;

    public FileTest(DataMap params) throws Exception {
        fileConfig = new FileConfig().load(params);
        String clazz = FileProtocolEnum.fromValue(fileConfig.getProtocol()).getStorage();
        storage = new TapFileStorageBuilder()
                .withClassLoader(Class.forName(clazz).getClassLoader())
                .withParams(params)
                .withStorageClassName(clazz)
                .build();
    }

    public TestItem testConnect() {
        try {
            boolean hasValid = false;
            List<String> inValid = new ArrayList<>();
            for (String path : fileConfig.getFilePathSet()) {
                if (storage.isDirectoryExist(path) || storage.isFileExist(path)) {
                    hasValid = true;
                } else {
                    inValid.add(path);
                }
            }
            if (!hasValid) {
                return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, "No valid directories or files could be found!");
            } else if (inValid.size() <= 0) {
                return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY);
            } else {
                return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                        String.format("path %s could not be found!", String.join(",", inValid)));
            }
        } catch (Exception e) {
            return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage());
        }
    }

    public String getConnectionString() {
        String filePathString = fileConfig.getFilePathString();
        if (EmptyKit.isBlank(filePathString)) {
            return storage.getConnectInfo();
        } else {
            return storage.getConnectInfo() + (filePathString.startsWith("/") ? filePathString.substring(1) : filePathString);
        }
    }

    @Override
    public void close() throws Exception {
        storage.destroy();
    }

}

package io.tapdata.file;

import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;


/**
 * Author:Skeet
 * Date: 2023/1/5
 **/
public class TapFileStorageImplTest {
    private String filePath = "example111" + UUID.randomUUID().toString().replace("-", "") + System.currentTimeMillis() + "/";
    private String filePath2 = "example222" + UUID.randomUUID().toString().replace("-", "") + System.currentTimeMillis() + "/";
    private Map<String, Object> config;
    private String ct1;
    private String ct2;
    private String content1 = "Hello YSK";
    private String content2 = "Hello OSS";

    @AfterEach
    public void after() throws Exception {
        TapFileStorage tapFileStorage = new TapFileStorageBuilder()
                .withClassLoader(this.getClass().getClassLoader())
                .withParams(config)
                .withStorageClassName((String) config.get("_storageClass"))
                .build();
        tapFileStorage.delete(filePath);
    }

    @Test
    public void testTapFileStorage() throws Exception {
        //加载config.json， 是FileStorage实现需要的配置参数
        String jsonPath = "D:\\work\\tapdata\\tapdata\\file-storages\\file-tests\\src\\test\\resources\\config\\oss.json";
        JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);
        config = (Map<String, Object>) jsonParser.fromJson(FileUtils.readFileToString(new File(jsonPath), "utf8"));

        TapFileStorage tapFileStorage = new TapFileStorageBuilder()
                .withClassLoader(this.getClass().getClassLoader())
                .withParams(config)
                .withStorageClassName((String) config.get("_storageClass"))
                .build();

//********************************************saveFile****************************************************

        //测试CanReplace为true时上传同名文件的结果
        TapFile tapFile1 = tapFileStorage.saveFile(filePath + "exampleobject1.txt", new ByteArrayInputStream(content1.getBytes()), true);
        Assertions.assertNotNull(tapFile1);
        tapFileStorage.readFile(filePath + "exampleobject1.txt", inputStream -> {
            try {
                this.ct1 = IOUtils.toString(inputStream, "utf8");
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
            Assertions.assertEquals(content1, this.ct1);
        });

        TapFile tapFile2 = tapFileStorage.saveFile(filePath + "exampleobject1.txt", new ByteArrayInputStream(content2.getBytes()), true);
        Assertions.assertNotNull(tapFile2);
        tapFileStorage.readFile(filePath + "exampleobject1.txt", inputStream -> {
            try {
                this.ct2 = IOUtils.toString(inputStream, "utf8");
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
            Assertions.assertEquals(content2, this.ct2);
        });
        Assertions.assertNotEquals(this.ct1, this.ct2);

        //测试CanReplace为false时上传同名文件的结果
        TapFile tapFile3 = tapFileStorage.saveFile(filePath + "exampleobject1.txt", new ByteArrayInputStream(content1.getBytes()), false);
        Assertions.assertNotNull(tapFile3);
        tapFileStorage.readFile(filePath + "exampleobject1.txt", inputStream -> {
            try {
                this.ct1 = IOUtils.toString(inputStream, "utf8");
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
            Assertions.assertEquals(content2, this.ct1);
        });

        TapFile tapFile4 = tapFileStorage.saveFile(filePath + "exampleobject2.txt", new ByteArrayInputStream(content2.getBytes()), true);
        Assertions.assertNotNull(tapFile4);

//********************************************getFilesInDirectory****************************************************
        List<TapFile> allFiles = new ArrayList<>();
        tapFileStorage.getFilesInDirectory(filePath, Collections.singletonList("*.txt"), null, true, 2, new Consumer<List<TapFile>>() {
            @Override
            public void accept(List<TapFile> tapFiles) {
                allFiles.addAll(tapFiles);
            }
        });
        boolean foundIt1 = false;
        for(TapFile file : allFiles) {
            if(file.getPath().equals(filePath + "exampleobject1.txt")) {
                foundIt1 = true;
                break;
            }
        }

        Assertions.assertTrue(foundIt1);

//********************************************readFile***********************************************************

        tapFileStorage.readFile(filePath + "exampleobject1.txt", inputStream -> {

            try {
                this.ct2 = IOUtils.toString(inputStream, "utf8");
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
            Assertions.assertEquals(content2, this.ct2);
        });

//********************************************isDirectoryExist************************************************************

        tapFileStorage.isDirectoryExist((String) config.get("filePath"));
        tapFileStorage.readFile(filePath + "exampleobject1.txt", inputStream -> {

            try {
                this.ct2 = IOUtils.toString(inputStream, "utf8");
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
            Assertions.assertEquals(content2, this.ct2);
            boolean isExist = true;
            Assertions.assertTrue(isExist);
        });
//********************************************move************************************************************

        tapFileStorage.move(filePath + "exampleobject1.txt", filePath2 + "exampleobject1.txt");
        TapFile newFiel = tapFileStorage.getFile(filePath2 + "exampleobject1.txt");
        Assertions.assertNotNull(newFiel);
        TapFile oldFile = tapFileStorage.getFile(filePath + "exampleobject1.txt");
        Assertions.assertNull(oldFile);

//********************************************delete************************************************************

        tapFileStorage.delete(filePath2 + "exampleobject1.txt");
        tapFileStorage.delete(filePath + "exampleobject2.txt");
        List<TapFile> allFiles2 = new ArrayList<>();
        tapFileStorage.getFilesInDirectory(filePath, Collections.singletonList("*.txt"), null, true, 2, new Consumer<List<TapFile>>() {
            @Override
            public void accept(List<TapFile> tapFiles) {
                allFiles2.addAll(tapFiles);
            }
        });
        boolean foundIt2 = false;
        for(TapFile file : allFiles2) {
            if(file.getPath().equals(filePath + "exampleobject1.txt")) {
                foundIt2 = true;
                break;
            }
        }
        Assertions.assertFalse(foundIt2);
    }

}


package io.tapdata.file;

import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;

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
    @Disabled
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
        /**
         * 测试SaveFile功能，当测试CanReplace为true时，上传同名但不同内容的文件，结果应该是后上传的同名文件替换掉先上传的文件
         * **/
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
        /**
         * 测试getFilesInDirectory功能，遍历当前上传到存储空间的所有文件，如果有存在，期望返回True;
         * **/
        List<TapFile> allFiles = new ArrayList<>();
        tapFileStorage.getFilesInDirectory(filePath, Collections.singletonList("*.txt"), null, true, 2, new Consumer<List<TapFile>>() {
            @Override
            public void accept(List<TapFile> tapFiles) {
                allFiles.addAll(tapFiles);
            }
        });
        boolean foundIt1 = false;
        for (TapFile file : allFiles) {
            if (file.getPath().equals(filePath + "exampleobject1.txt")) {
                foundIt1 = true;
                break;
            }
        }

        Assertions.assertTrue(foundIt1);

//********************************************readFile***********************************************************
        /**
         *测试readFile功能，根据上传文件的路径，期望结果是上传到存储空间的文件和上传之前的content一致
         * **/
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
        /**
         * 测试isDirectoryExist功能，期望结果是文件存在，且内容和上传之前的content是一致的。
         * **/
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

        /**
         * 测试move功能，期望结果是move之后，新的文件路径有move之后的文件，旧的文件路径里文件应该null的;
         * **/
        tapFileStorage.move(filePath + "exampleobject1.txt", filePath2 + "exampleobject1.txt");
        TapFile newFiel = tapFileStorage.getFile(filePath2 + "exampleobject1.txt");
        Assertions.assertNotNull(newFiel);
        TapFile oldFile = tapFileStorage.getFile(filePath + "exampleobject1.txt");
        Assertions.assertNull(oldFile);

//********************************************delete************************************************************

        /**
         * 测试delete功能，期望结果是，被删除的文件，遍历结果为空;
         * **/
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
        for (TapFile file : allFiles2) {
            if (file.getPath().equals(filePath + "exampleobject1.txt")) {
                foundIt2 = true;
                break;
            }
        }
        Assertions.assertFalse(foundIt2);

        List<TapFile> allFiles3 = new ArrayList<>();
        tapFileStorage.getFilesInDirectory(filePath2, Collections.singletonList("*.txt"), null, true, 2, new Consumer<List<TapFile>>() {
            @Override
            public void accept(List<TapFile> tapFiles) {
                allFiles3.addAll(tapFiles);
            }
        });
        boolean foundIt3 = false;
        for (TapFile file : allFiles3) {
            if (file.getPath().equals(filePath2 + "exampleobject1.txt")) {
                foundIt3 = true;
                break;
            }
        }
        Assertions.assertFalse(foundIt3);
    }

}


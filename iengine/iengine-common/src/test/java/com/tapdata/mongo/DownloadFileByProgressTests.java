package com.tapdata.mongo;

import org.apache.commons.io.input.BrokenInputStream;
import org.apache.commons.io.input.NullInputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class DownloadFileByProgressTests {

    private RestTemplateOperator restTemplateOperatorUnderTest;

    @Before
    public void setUp() throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Class<?> myClass = RestTemplateOperator.class;
        Constructor<?> constructor = myClass.getDeclaredConstructor(); // 获取私有构造方法
        constructor.setAccessible(true); // 设置构造方法为可访问
        restTemplateOperatorUnderTest = (RestTemplateOperator) constructor.newInstance(); // 创建对象
    }

    @Test
    public void testDownloadFileByProgress() throws Exception {
        String mockString = "s";
        for(int i = 1; i < 1024;i++){
            mockString = mockString+"s";
        }
        final int mockFileSize = mockString.getBytes().length;
        final InputStream mockSource = new ByteArrayInputStream(mockString.getBytes());
        final File mockFile = new File("filename.txt");
         RestTemplateOperator.Callback mockCallback = new RestTemplateOperator.Callback() {
            @Override
            public void needDownloadPdkFile(boolean flag) throws IOException {

            }

            @Override
            public void onProgress(long fileSize, long progress) throws IOException {
                Assert.assertEquals(100,progress);
                Assert.assertEquals(mockFileSize, fileSize);
            }

            @Override
            public void onFinish(String downloadSpeed) throws IOException {
                Assert.assertNotNull(downloadSpeed);
            }

            @Override
            public void onError(IOException ex) throws IOException {
                Assert.assertNull(ex);

            }
        };
        // Run the test
        restTemplateOperatorUnderTest.downloadFileByProgress(mockCallback, mockSource, mockFile, mockFileSize);
    }

    @Test
    public void testDownloadFileByProgress_EmptySource() throws Exception {
        // Setup
        RestTemplateOperator.Callback mockCallback = new RestTemplateOperator.Callback() {
            @Override
            public void needDownloadPdkFile(boolean flag) throws IOException {

            }

            @Override
            public void onProgress(long fileSize, long progress) throws IOException {
                Assert.assertEquals(100,progress);
                Assert.assertEquals(0,fileSize);
            }

            @Override
            public void onFinish(String downloadSpeed) throws IOException {
                Assert.assertNotNull(downloadSpeed);
            }

            @Override
            public void onError(IOException ex) throws IOException {
                Assert.assertNull(ex);

            }
        };
        final InputStream source = new NullInputStream();
        final File file = new File("filename.txt");
        restTemplateOperatorUnderTest.downloadFileByProgress(mockCallback, source, file, 0L);

    }

    @Test(expected = IOException.class)
    public void testDownloadFileByProgress_BrokenSource() throws Exception {
        // Setup
        RestTemplateOperator.Callback mockCallback = new RestTemplateOperator.Callback() {
            @Override
            public void needDownloadPdkFile(boolean flag) throws IOException {

            }

            @Override
            public void onProgress(long fileSize, long progress) throws IOException {
            }

            @Override
            public void onFinish(String downloadSpeed) throws IOException {
            }

            @Override
            public void onError(IOException ex) throws IOException {
               throw ex;
            }
        };
        final InputStream source = new BrokenInputStream();
        final File file = new File("filename.txt");
        restTemplateOperatorUnderTest.downloadFileByProgress(mockCallback, source, file, 0L);
    }

    @Test(expected = RuntimeException.class)
    public void testDownloadFileByProgress_NullFile() throws Exception {
        // Setup
        RestTemplateOperator.Callback mockCallback = new RestTemplateOperator.Callback() {
            @Override
            public void needDownloadPdkFile(boolean flag) throws IOException {

            }

            @Override
            public void onProgress(long fileSize, long progress) throws IOException {
            }

            @Override
            public void onFinish(String downloadSpeed) throws IOException {
            }

            @Override
            public void onError(IOException ex) throws IOException {
                throw ex;
            }
        };
        final InputStream source = new NullInputStream();
        restTemplateOperatorUnderTest.downloadFileByProgress(mockCallback, source, null, 0L);
    }

    @Test(expected = RuntimeException.class)
    public void testDownloadFileByProgress_NullCallBack() throws Exception {
        // Setup
        RestTemplateOperator.Callback mockCallback = null;
        final File mockFile = new File("filename.txt");
        final InputStream source = new NullInputStream();
        restTemplateOperatorUnderTest.downloadFileByProgress(mockCallback, source, mockFile, 0L);
    }
}

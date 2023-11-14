package com.tapdata.mongo;

import com.google.common.collect.ImmutableList;
import io.tapdata.exception.ManagementException;
import org.apache.commons.io.input.BrokenInputStream;
import org.apache.commons.io.input.NullInputStream;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.http.client.MockClientHttpRequest;
import org.springframework.mock.http.client.MockClientHttpResponse;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.*;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RestTemplateOperatorTest {
    @Mock
    private RestTemplate mockRestTemplate;

    private RestTemplateOperator restTemplateOperatorUnderTest;

    @Before
    public void setUp() throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Class<?> myClass = RestTemplateOperator.class;
        Constructor<?> constructor = myClass.getDeclaredConstructor(); // 获取私有构造方法
        constructor.setAccessible(true); // 设置构造方法为可访问
        restTemplateOperatorUnderTest = (RestTemplateOperator) constructor.newInstance(); // 创建对象
        ReflectionTestUtils.setField(restTemplateOperatorUnderTest, "baseURLs", Lists.newArrayList("test1","test2"));
        ReflectionTestUtils.setField(restTemplateOperatorUnderTest,"restTemplate",mockRestTemplate);
    }

    /**
     * Normal use case
     * @throws Exception
     */
    @Test
    public void testDownloadFile() throws Exception {
        // input param
        final Map<String, Object> params = new HashMap<>();
        RestTemplateOperator.Callback inputCallback = new RestTemplateOperator.Callback() {
            @Override
            public void needDownloadPdkFile(boolean flag) throws IOException {

            }

            @Override
            public void onProgress(long fileSize, long progress) throws IOException {
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
        // expected data
        final File expectedResult = new File("filename.bak");

        when(mockRestTemplate.execute(any(URI.class), any(HttpMethod.class),
                any(RequestCallback.class), any(ResponseExtractor.class))).thenAnswer(invocationOnMock -> {
            //Simulate http to obtain file stream and write to local file
            MockClientHttpResponse mockClientHttpResponse = new MockClientHttpResponse(new byte[0], HttpStatus.OK);
            ResponseExtractor responseExtractor = invocationOnMock.getArgumentAt(3,ResponseExtractor.class);
            responseExtractor.extractData(mockClientHttpResponse);
            return true;
        });
        // Run the test，actual data
        final File result = restTemplateOperatorUnderTest.downloadFile(params, "resource", "filename", "cookies", "region",
                inputCallback);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    /**
     * Test ClientHttpRequest Check parameters case
     * @throws Exception
     */
    @Test
    public void testDownloadFile_CheckClientHttpRequest() throws Exception {
        // input param
        final Map<String, Object> params = new HashMap<>();
        RestTemplateOperator.Callback inputCallback = null;
        // expected data
        MockClientHttpRequest mockClientHttpRequest = new MockClientHttpRequest();
        when(mockRestTemplate.execute(any(URI.class), any(HttpMethod.class),
                any(RequestCallback.class), any(ResponseExtractor.class))).thenAnswer(invocationOnMock -> {
            //Simulate http to obtain file stream and write to local file
            RequestCallback requestCallback = invocationOnMock.getArgumentAt(2,RequestCallback.class);
            requestCallback.doWithRequest(mockClientHttpRequest);
            return false;
        });
        // Run the test，actual data
        List<MediaType> expectedMedia= ImmutableList.of(
                MediaType.APPLICATION_OCTET_STREAM,
                new MediaType("application", "*+json"));
        LinkedList<String> expectedCookies = new LinkedList<>();
        expectedCookies.add("cookies");
        LinkedList<String> expectedRegion = new LinkedList<>();
        expectedRegion.add("region");
        final File result = restTemplateOperatorUnderTest.downloadFile(params, "resource", "filename", "cookies", "region",
                inputCallback);
        Object resultCookie = mockClientHttpRequest.getHeaders().get("Cookie");
        Object resultJobTags = mockClientHttpRequest.getHeaders().get("jobTags");
        List<MediaType> resultMedia =  mockClientHttpRequest.getHeaders().getAccept();
        // Verify the results
        assertEquals(expectedCookies,resultCookie);
        assertEquals(expectedRegion,resultJobTags);
        assertEquals(expectedMedia,resultMedia);
    }

    /**
     * input region is null case
     * @throws Exception
     */
    @Test
    public void testDownloadFile_inputRegionNull() throws Exception {
        // input param
        final Map<String, Object> params = new HashMap<>();
        RestTemplateOperator.Callback inputCallback = null;
        // expected data
        MockClientHttpRequest mockClientHttpRequest = new MockClientHttpRequest();
        when(mockRestTemplate.execute(any(URI.class), any(HttpMethod.class),
                any(RequestCallback.class), any(ResponseExtractor.class))).thenAnswer(invocationOnMock -> {
            //Simulate http to obtain file stream and write to local file
            RequestCallback requestCallback = invocationOnMock.getArgumentAt(2,RequestCallback.class);
            requestCallback.doWithRequest(mockClientHttpRequest);
            return false;
        });
        // Run the test，actual data
        List<MediaType> expectedMedia= ImmutableList.of(
                MediaType.APPLICATION_OCTET_STREAM,
                new MediaType("application", "*+json"));
        LinkedList<String> expectedCookies = new LinkedList<>();
        expectedCookies.add("cookies");
        LinkedList<String> expectedRegion = new LinkedList<>();
        expectedRegion.add("region");
        restTemplateOperatorUnderTest.downloadFile(params, "resource", "filename", "cookies", null,
                inputCallback);
        Object resultCookie = mockClientHttpRequest.getHeaders().get("Cookie");
        Object resultJobTags = mockClientHttpRequest.getHeaders().get("jobTags");
        List<MediaType> resultMedia =  mockClientHttpRequest.getHeaders().getAccept();
        // Verify the results
        assertEquals(expectedCookies,resultCookie);
        assertNull(resultJobTags);
        assertEquals(expectedMedia,resultMedia);
    }

    /**
     * input cookies is null case
     * @throws Exception
     */
    @Test
    public void testDownloadFile_inputCookiesNull() throws Exception {
        // input param
        final Map<String, Object> params = new HashMap<>();
        RestTemplateOperator.Callback inputCallback = null;
        // expected data
        MockClientHttpRequest mockClientHttpRequest = new MockClientHttpRequest();
        when(mockRestTemplate.execute(any(URI.class), any(HttpMethod.class),
                any(RequestCallback.class), any(ResponseExtractor.class))).thenAnswer(invocationOnMock -> {
            //Simulate http to obtain file stream and write to local file
            RequestCallback requestCallback = invocationOnMock.getArgumentAt(2,RequestCallback.class);
            requestCallback.doWithRequest(mockClientHttpRequest);
            return false;
        });
        // Run the test，actual data
        restTemplateOperatorUnderTest.downloadFile(params, "resource", "filename", null, "region",
                inputCallback);
        // Verify the results
        assertTrue(mockClientHttpRequest.getHeaders().isEmpty());
    }

    /**
     * restTemplate request return null
     * @throws Exception
     */
    @Test
    public void testDownloadFile_RestTemplateReturnsNull() throws Exception {
        // Setup
        final Map<String, Object> params = new HashMap<>();
        final RestTemplateOperator.Callback callback = null;
        when(mockRestTemplate.execute(any(URI.class), any(HttpMethod.class),
                any(RequestCallback.class), any(ResponseExtractor.class))).thenReturn(null);

        // Run the test
        final File result = restTemplateOperatorUnderTest.downloadFile(params, "resource", "path", "cookies", "region",
                callback);

        // Verify the results
        assertNull(result);
    }

    /**
     * restTemplate request failed
     * @throws Exception
     */
    @Test(expected = ManagementException.class)
    public void testDownloadFile_RestTemplateThrowsRestClientException() throws Exception {
        // Setup
        final Map<String, Object> params = new HashMap<>();
        final RestTemplateOperator.Callback callback = null;
        when(mockRestTemplate.execute(any(URI.class), any(HttpMethod.class),
                any(RequestCallback.class), any(ResponseExtractor.class))).thenThrow(HttpClientErrorException.class);

        // Run the test
        restTemplateOperatorUnderTest.downloadFile(params, "resource", "path", "cookies", "region", callback);
    }

    /**
     * Normal use case
     * @throws Exception
     */
    @Test
    public void testDownloadFileByProgress() throws Exception {
        // input param
        String inputString = "s";
        for(int i = 1; i < 1024;i++){
            inputString = inputString+"s";
        }
        final int inputFileSize = inputString.getBytes().length;
        final InputStream inputSource = new ByteArrayInputStream(inputString.getBytes());
        final File inputFile = new File("filename.txt");
        RestTemplateOperator.Callback inputCallback = new RestTemplateOperator.Callback() {
            @Override
            public void needDownloadPdkFile(boolean flag) throws IOException {

            }

            @Override
            public void onProgress(long fileSize, long progress) throws IOException {
                Assert.assertEquals(100,progress);
                Assert.assertEquals(inputFileSize, fileSize);
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
        restTemplateOperatorUnderTest.downloadFileByProgress(inputCallback, inputSource, inputFile, inputFileSize);
    }

    /**
     * inputSource is empty case
     * @throws Exception
     */
    @Test
    public void testDownloadFileByProgress_EmptySource() throws Exception {
        // input param
        RestTemplateOperator.Callback inputCallback = new RestTemplateOperator.Callback() {
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
        final InputStream inputSource = new NullInputStream();
        final File inputFile = new File("filename.txt");
        restTemplateOperatorUnderTest.downloadFileByProgress(inputCallback, inputSource, inputFile, 0L);

    }

    /**
     * simulation ioexception case
     * @throws Exception
     */
    @Test(expected = IOException.class)
    public void testDownloadFileByProgress_BrokenSource() throws Exception {
        RestTemplateOperator.Callback inputCallback = new RestTemplateOperator.Callback() {
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
        restTemplateOperatorUnderTest.downloadFileByProgress(inputCallback, source, file, 0L);
    }

    /**
     * Input file is empty case
     * @throws Exception
     */
    @Test(expected = RuntimeException.class)
    public void testDownloadFileByProgress_NullFile() throws Exception {
        // Setup
        RestTemplateOperator.Callback inputCallback = new RestTemplateOperator.Callback() {
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
        restTemplateOperatorUnderTest.downloadFileByProgress(inputCallback, source, null, 0L);
    }
}

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * @author samuel
 * @Description
 * @create 2022-12-30 17:43
 **/
public class DorisStreamTest {
	// FE IP Address
	private final static String HOST = "139.198.127.226";
	// FE port
	private final static int PORT = 30476;
	// db name
	private final static String DATABASE = "demo";
	// table name
	private final static String TABLE = "test";
	//user name
	private final static String USER = "root";
	//user password
	private final static String PASSWD = "";
	//The path of the local file to be imported
	private final static String LOAD_FILE_NAME = "c:/es/1.csv";

	//http path of stream load task submission
	private final static String loadUrl = String.format("http://%s:%s/api/%s/%s/_stream_load",
			HOST, PORT, DATABASE, TABLE);

	//Build http client builder
	private final static HttpClientBuilder httpClientBuilder = HttpClients
			.custom()
			.setRedirectStrategy(new DefaultRedirectStrategy() {
				@Override
				protected boolean isRedirectable(String method) {
					// If the connection target is FE, you need to deal with 307 redirect。
					return true;
				}
			});

	private String basicAuthHeader(String username, String password) {
		final String tobeEncode = username + ":" + password;
		byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
		return "Basic " + new String(encoded);
	}

	public void loadJson(String jsonData) throws Exception {
		try (CloseableHttpClient client = httpClientBuilder.build()) {
			HttpPut put = new HttpPut(loadUrl);
			put.removeHeaders(HttpHeaders.CONTENT_LENGTH);
			put.removeHeaders(HttpHeaders.TRANSFER_ENCODING);
			put.setHeader(HttpHeaders.EXPECT, "100-continue");
			put.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(USER, PASSWD));

			// You can set stream load related properties in the Header, here we set label and column_separator.
			put.setHeader("label", UUID.randomUUID().toString());
			put.setHeader("column_separator", ",");
			put.setHeader("format", "json");

			// Set up the import file. Here you can also use StringEntity to transfer arbitrary data.
			StringEntity entity = new StringEntity(jsonData);
			put.setEntity(entity);

			try (CloseableHttpResponse response = client.execute(put)) {
				String loadResult = "";
				if (response.getEntity() != null) {
					loadResult = EntityUtils.toString(response.getEntity());
				}

				final int statusCode = response.getStatusLine().getStatusCode();
				if (statusCode != 200) {
					throw new IOException(String.format("Stream load failed. status: %s load result: %s", statusCode, loadResult));
				}
				System.out.println("Get load result: " + loadResult);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		DorisStreamTest dorisStreamTest = new DorisStreamTest();
		//file load
		//File file = new File(LOAD_FILE_NAME);
		//loader.load(file);
		//json load
		String jsonData = "[{\"_id\":\"63ae86cb399dff531d4165cf\",\"id\":1.0,\"name\":\"test1\",\"age\":12.0,\"__DORIS_DELETE_SIGN__\":\"0\"}]";
		dorisStreamTest.loadJson(jsonData);
	}
}

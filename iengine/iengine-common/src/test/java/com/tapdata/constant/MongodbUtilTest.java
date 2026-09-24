package com.tapdata.constant;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class MongodbUtilTest {
	@Test
	void countShouldEstimateOnlyWhenFilterIsNull() {
		MongoClient client = mock(MongoClient.class);
		MongoDatabase database = mock(MongoDatabase.class);
		MongoCollection<Document> collection = mock(MongoCollection.class);
		when(client.getDatabase("db")).thenReturn(database);
		when(database.getCollection("collection")).thenReturn(collection);
		when(collection.estimatedDocumentCount()).thenReturn(10L);
		when(collection.countDocuments(any(Document.class))).thenReturn(9L);

		assertEquals(10L, MongodbUtil.getCollectionNotAggregateCountByTableName(client, "db", "collection", null));
		assertEquals(9L, MongodbUtil.getCollectionNotAggregateCountByTableName(client, "db", "collection", new Document()));
		verify(collection).estimatedDocumentCount();
		verify(collection).countDocuments(any(Document.class));
	}

    @DisplayName("test get mongoUri without additionalString")
    @Test
    void test1() {
        Map<String, Object> config = new HashMap<>();
        config.put("user", "testUser");
        config.put("password", "testPassword");
        config.put("host", "localhost");
        config.put("database", "testDatabase");
        String mongoUri = MongodbUtil.getUri(config);
        assertEquals("mongodb://testUser:testPassword@localhost/testDatabase", mongoUri);
    }

    @DisplayName("test get mongoUri with additionalString")
    @Test
    void test2() {
        Map<String, Object> config = new HashMap<>();
        config.put("user", "testUser");
        config.put("password", "testPassword");
        config.put("host", "localhost");
        config.put("database", "testDatabase");
        config.put("additionalString", "authSource=admin");
        String mongoUri = MongodbUtil.getUri(config);
        assertEquals("mongodb://testUser:testPassword@localhost/testDatabase?authSource=admin", mongoUri);
    }
    @DisplayName("test get mongoUri with user and password")
    @Test
    void test3(){
        Map<String, Object> config = new HashMap<>();
        config.put("host", "localhost");
        config.put("database", "testDatabase");
        config.put("additionalString", "authSource=admin");
        String mongoUri = MongodbUtil.getUri(config);
        assertEquals("mongodb://localhost/testDatabase?authSource=admin", mongoUri);
    }
    @DisplayName("test get MongoUri By Uri")
    @Test
    void test4(){
        Map<String, Object> config = new HashMap<>();
        String uri="mongodb://localhost/testDatabase?authSource=admin";
        config.put("isUri",true);
        config.put("uri",uri);
        String uriResult = MongodbUtil.getUri(config);
        assertEquals(uri,uriResult);
    }
}

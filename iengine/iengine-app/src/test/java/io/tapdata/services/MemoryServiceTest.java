package io.tapdata.services;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.PersistenceStorage;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.task.dto.CacheStatistics;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.HazelcastMergeNode;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import io.tapdata.pdk.core.api.PDKIntegration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class MemoryServiceTest {

    private MemoryService memoryService;
    private ExternalStorageDto externalStorageDto;

    @BeforeEach
    void setUp() {
        memoryService = new MemoryService();
        externalStorageDto = new ExternalStorageDto();
        externalStorageDto.setUri("mongodb://localhost:27017/test");
    }

    @Test
    void testMergeCacheManager_WhenRunning_ShouldReturnLocalAndRemoteCache() {
        // Arrange
        List<String> keys = Arrays.asList("key1", "key2");
        Map<String, String> info = new HashMap<>();
        info.put("node1", "table1");
        info.put("node2", "table2");

        DataMap localCache = DataMap.create();
        DataMap localNode1Stats = DataMap.create();
        localNode1Stats.put("node1",CacheStatistics.createLocalCache(100, 10, 1000));
        localCache.put("key1", localNode1Stats);
        localCache.put("removed", "should be removed");

        Map<String, Object> remoteStats1 = new HashMap<>();
        remoteStats1.put("size", 200L);
        remoteStats1.put("count", 20L);
        remoteStats1.put("uri", "mongodb://localhost:27017/test");
        remoteStats1.put("mode", "MongoDB");

        Map<String, Object> remoteStats2 = new HashMap<>();
        remoteStats2.put("size", 300L);
        remoteStats2.put("count", 30L);
        remoteStats2.put("uri", "mongodb://localhost:27017/test");
        remoteStats2.put("mode", "MongoDB");

        try (MockedStatic<PDKIntegration> pdkMock = mockStatic(PDKIntegration.class);
             MockedStatic<HazelcastMergeNode> mergeMock = mockStatic(HazelcastMergeNode.class);
             MockedStatic<PersistenceStorage> persistenceMock = mockStatic(PersistenceStorage.class)) {

            PersistenceStorage persistenceStorage = mock(PersistenceStorage.class);

            pdkMock.when(() -> PDKIntegration.outputMemoryFetchersInDataMap(keys, null, null))
                    .thenReturn(localCache);
            mergeMock.when(() -> HazelcastMergeNode.getMergeCacheName("node1", "table1"))
                    .thenReturn("cache1");
            mergeMock.when(() -> HazelcastMergeNode.getMergeCacheName("node2", "table2"))
                    .thenReturn("cache2");
            persistenceMock.when(PersistenceStorage::getInstance).thenReturn(persistenceStorage);
            when(persistenceStorage.getStatistics(eq(ConstructType.IMAP), eq(String.valueOf("cache1".hashCode()))))
                    .thenReturn(remoteStats1);
            when(persistenceStorage.getStatistics(eq(ConstructType.IMAP), eq(String.valueOf("cache2".hashCode()))))
                    .thenReturn(remoteStats2);

            // Act
            DataMap result = memoryService.mergeCacheManager(keys, info, true, externalStorageDto, null, null);

            // Assert
            assertNotNull(result);
            assertFalse(result.containsKey("removed"), "Should remove 'removed' key from local cache");
            assertTrue(result.containsKey("node1"), "Should contain remote cache for node1");
            assertTrue(result.containsKey("node2"), "Should contain remote cache for node2");

            List<CacheStatistics> node1Stats = (List<CacheStatistics>) result.get("node1");
            assertEquals(2, node1Stats.size());
            verify(persistenceStorage, never()).destroy(anyString(), any(ConstructType.class), anyString());
        }
    }

    @Test
    void testMergeCacheManager_WhenNotRunning_ShouldInitializeAndDestroyStorage() {
        // Arrange
        List<String> keys = Arrays.asList("key1");
        Map<String, String> info = new HashMap<>();
        info.put("node1", "table1");

        Map<String, Object> remoteStats = new HashMap<>();
        remoteStats.put("size", 500L);
        remoteStats.put("count", 50L);
        remoteStats.put("uri", "mongodb://localhost:27017/test");
        remoteStats.put("mode", "MongoDB");

        try (MockedStatic<PDKIntegration> pdkMock = mockStatic(PDKIntegration.class);
             MockedStatic<HazelcastMergeNode> mergeMock = mockStatic(HazelcastMergeNode.class);
             MockedStatic<HazelcastUtil> hazelcastMock = mockStatic(HazelcastUtil.class);
             MockedStatic<ExternalStorageUtil> storageMock = mockStatic(ExternalStorageUtil.class);
             MockedStatic<PersistenceStorage> persistenceMock = mockStatic(PersistenceStorage.class)) {

            HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);
            Config config = mock(Config.class);
            PersistenceStorage persistenceStorage = mock(PersistenceStorage.class);

            hazelcastMock.when(HazelcastUtil::getInstance).thenReturn(hazelcastInstance);
            when(hazelcastInstance.getConfig()).thenReturn(config);
            mergeMock.when(() -> HazelcastMergeNode.getMergeCacheName("node1", "table1"))
                    .thenReturn("cache1");
            persistenceMock.when(PersistenceStorage::getInstance).thenReturn(persistenceStorage);
            when(persistenceStorage.getStatistics(eq(ConstructType.IMAP), eq(String.valueOf("cache1".hashCode()))))
                    .thenReturn(remoteStats);

            // Act
            DataMap result = memoryService.mergeCacheManager(keys, info, false, externalStorageDto, null, null);

            // Assert
            assertNotNull(result);
            assertTrue(result.containsKey("node1"));

            List<CacheStatistics> stats = (List<CacheStatistics>) result.get("node1");
            assertEquals(500L, stats.get(0).getSize());
            assertEquals(50L, stats.get(0).getCount());

            storageMock.verify(() -> ExternalStorageUtil.initHZMapStorage(
                    eq(externalStorageDto),
                    eq(HazelcastMergeNode.class.getSimpleName()),
                    eq(String.valueOf("cache1".hashCode())),
                    eq(config)
            ), times(1));

            verify(persistenceStorage, times(1)).destroy(
                    eq(HazelcastMergeNode.class.getSimpleName()),
                    eq(ConstructType.IMAP),
                    eq(String.valueOf("cache1".hashCode()))
            );
        }
    }

    @Test
    void testMergeCacheManager_WhenStatisticsIsNull_ShouldSkipEntry() {
        // Arrange
        List<String> keys = Collections.emptyList();
        Map<String, String> info = new HashMap<>();
        info.put("node1", "table1");
        info.put("node2", "table2");

        Map<String, Object> remoteStats = new HashMap<>();
        remoteStats.put("size", 100L);
        remoteStats.put("count", 10L);
        remoteStats.put("uri", "mongodb://localhost:27017/test");
        remoteStats.put("mode", "MongoDB");

        try (MockedStatic<HazelcastMergeNode> mergeMock = mockStatic(HazelcastMergeNode.class);
             MockedStatic<PersistenceStorage> persistenceMock = mockStatic(PersistenceStorage.class)) {

            PersistenceStorage persistenceStorage = mock(PersistenceStorage.class);

            mergeMock.when(() -> HazelcastMergeNode.getMergeCacheName("node1", "table1"))
                    .thenReturn("cache1");
            mergeMock.when(() -> HazelcastMergeNode.getMergeCacheName("node2", "table2"))
                    .thenReturn("cache2");
            persistenceMock.when(PersistenceStorage::getInstance).thenReturn(persistenceStorage);
            when(persistenceStorage.getStatistics(eq(ConstructType.IMAP), eq(String.valueOf("cache1".hashCode()))))
                    .thenReturn(null);
            when(persistenceStorage.getStatistics(eq(ConstructType.IMAP), eq(String.valueOf("cache2".hashCode()))))
                    .thenReturn(remoteStats);

            // Act
            DataMap result = memoryService.mergeCacheManager(keys, info, true, externalStorageDto, null, null);

            // Assert
            assertNotNull(result);
            assertFalse(result.containsKey("node1"), "Should skip node1 when statistics is null");
            assertTrue(result.containsKey("node2"), "Should include node2 with valid statistics");
        }
    }


    @Test
    void testMergeCacheManager_WhenNotRunning_WithEmptyInfo_ShouldReturnEmptyDataMap() {
        // Arrange
        List<String> keys = Collections.emptyList();
        Map<String, String> info = new HashMap<>();

        // Act
        DataMap result = memoryService.mergeCacheManager(keys, info, false, externalStorageDto, null, null);

        // Assert
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

}

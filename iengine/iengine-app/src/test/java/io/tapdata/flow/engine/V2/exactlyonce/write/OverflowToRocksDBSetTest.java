package io.tapdata.flow.engine.V2.exactlyonce.write;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OverflowToRocksDBSetTest {

	private OverflowToRocksDBSet set;

	@AfterEach
	void tearDown() {
		if (set != null) {
			set.close();
		}
	}

	@Nested
	@DisplayName("constructor validation test")
	class ConstructorTest {
		@Test
		void testIllegalThreshold() {
			assertThrows(IllegalArgumentException.class, () -> new OverflowToRocksDBSet(0L, "x"));
			assertThrows(IllegalArgumentException.class, () -> new OverflowToRocksDBSet(-1L, "x"));
		}

		@Test
		void testBlankDbNameFallback() {
			set = new OverflowToRocksDBSet(10L, "  ");
			String dbName = (String) ReflectionTestUtils.getField(set, "dbName");
			assertNotNull(dbName);
			assertFalse(dbName.isBlank());
		}

		@Test
		void testDefaultConstructor() {
			set = new OverflowToRocksDBSet();
			assertEquals(OverflowToRocksDBSet.DEFAULT_MEMORY_THRESHOLD, ReflectionTestUtils.getField(set, "memoryThreshold"));
		}
	}

	@Nested
	@DisplayName("memory only behavior test")
	class MemoryOnlyTest {
		@Test
		void testAddAndContains() {
			set = new OverflowToRocksDBSet(10L, UUID.randomUUID().toString());
			assertTrue(set.add("a"));
			assertFalse(set.add("a"));
			assertTrue(set.contains("a"));
			assertFalse(set.contains("missing"));
			assertEquals(1, set.size());
			assertFalse(set.isEmpty());
			assertNull(ReflectionTestUtils.getField(set, "db"));
		}

		@Test
		void testContainsRejectsNonString() {
			set = new OverflowToRocksDBSet(10L, UUID.randomUUID().toString());
			set.add("a");
			assertFalse(set.contains(1));
			assertFalse(set.contains(null));
		}

		@Test
		void testIteratorBeforeSpill() {
			set = new OverflowToRocksDBSet(10L, UUID.randomUUID().toString());
			set.add("a");
			set.add("b");
			int count = 0;
			for (String s : set) {
				assertNotNull(s);
				count++;
			}
			assertEquals(2, count);
		}

		@Test
		void testAddNullRejectedBeforeSpill() {
			set = new OverflowToRocksDBSet(10L, UUID.randomUUID().toString());
			assertThrows(NullPointerException.class, () -> set.add(null));
			assertEquals(0, set.size());
			assertTrue(set.isEmpty());
		}
	}

	@Nested
	@DisplayName("overflow to rocksdb test")
	class OverflowTest {
		@Test
		void testSpillTriggeredAfterThreshold() {
			set = new OverflowToRocksDBSet(3L, UUID.randomUUID().toString());
			assertTrue(set.add("a"));
			assertTrue(set.add("b"));
			assertTrue(set.add("c"));
			assertNull(ReflectionTestUtils.getField(set, "db"));
			assertTrue(set.add("d"));
			assertNotNull(ReflectionTestUtils.getField(set, "db"));
			File dbDir = (File) ReflectionTestUtils.getField(set, "dbDir");
			assertNotNull(dbDir);
			assertTrue(dbDir.exists());
			assertEquals(4, set.size());
			assertTrue(set.contains("a"));
			assertTrue(set.contains("d"));
		}

		@Test
		void testAddDuplicateInMemoryAfterSpill() {
			set = new OverflowToRocksDBSet(2L, UUID.randomUUID().toString());
			set.add("a");
			set.add("b");
			set.add("c");
			assertFalse(set.add("a"));
			assertFalse(set.add("c"));
			assertEquals(3, set.size());
		}

		@Test
		void testIteratorAfterSpillThrows() {
			set = new OverflowToRocksDBSet(1L, UUID.randomUUID().toString());
			set.add("a");
			set.add("b");
			assertThrows(UnsupportedOperationException.class, () -> set.iterator());
		}

		@Test
		void testAddNullRejectedAfterSpill() {
			set = new OverflowToRocksDBSet(1L, UUID.randomUUID().toString());
			set.add("a");
			set.add("b");
			assertNotNull(ReflectionTestUtils.getField(set, "db"));
			assertThrows(NullPointerException.class, () -> set.add(null));
			assertEquals(2, set.size());
		}

		@Test
		void testClearResetsMemoryAndDisk() {
			String dbName = UUID.randomUUID().toString();
			set = new OverflowToRocksDBSet(1L, dbName);
			set.add("a");
			set.add("b");
			File dbDir = (File) ReflectionTestUtils.getField(set, "dbDir");
			assertTrue(dbDir.exists());
			set.clear();
			assertEquals(0, set.size());
			assertTrue(set.isEmpty());
			assertNull(ReflectionTestUtils.getField(set, "db"));
			assertFalse(dbDir.exists());
			assertTrue(set.add("x"));
		}

		@Test
		void testCloseDeletesDir() throws Exception {
			String dbName = UUID.randomUUID().toString();
			set = new OverflowToRocksDBSet(1L, dbName);
			set.add("a");
			set.add("b");
			File dbDir = (File) ReflectionTestUtils.getField(set, "dbDir");
			set.close();
			assertNull(ReflectionTestUtils.getField(set, "db"));
			assertNull(ReflectionTestUtils.getField(set, "dbDir"));
			assertFalse(dbDir.exists());
			FileUtils.deleteDirectory(dbDir.getParentFile());
			set = null;
		}
	}
}

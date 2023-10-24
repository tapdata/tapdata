package io.tapdata.utils;

import io.tapdata.modules.api.utils.APIUtils;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class APIUtilsTest {
	@Test
	public void testIdForList() {
		String id0 = APIUtils.idForList(null);
		assertEquals(APIUtils.idForString("null"), id0);
		String id = APIUtils.idForList(Arrays.asList());
		assertEquals(APIUtils.idForString("list: "), id);

		String id1 = APIUtils.idForList(Arrays.asList("fffff", "1", "aaa", "bbb", "zzz"));
		String id2 = APIUtils.idForList(Arrays.asList("fffff", "1", "aaa", "bbb", "zzz"));
		assertEquals(id1, id2);

		String id5 = APIUtils.idForList(Arrays.asList("fffff", "1", "aaa", "bbb", "zzz"));
		String id6 = APIUtils.idForList(Arrays.asList("fffff", "1 ", "aaa", "bbb", "zzz"));
		assertNotEquals(id5, id6);

		String id3 = APIUtils.idForList(Arrays.asList("fffff", "1", "aaa", "bbb", "zzz"));
		String id4 = APIUtils.idForList(Arrays.asList("fffff", "2", "aaa", "bbb", "zzz"));
		assertNotEquals(id3, id4);
	}
}

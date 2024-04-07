package io.tapdata.flow.engine.V2.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author samuel
 * @Description
 * @create 2024-04-03 19:39
 **/
@DisplayName("Class StateMapUtil Test")
class StateMapUtilTest {
	@Test
	@DisplayName("Method encodeDotAndDollar test")
	void testEncodeDotAndDollar() {
		String actual = StateMapUtil.encodeDotAndDollar("a1b#");
		assertEquals("a1b#", actual);

		actual = StateMapUtil.encodeDotAndDollar("$$$a$1$$#$$");
		assertEquals("\\u0024\\u0024\\u0024a\\u00241\\u0024\\u0024#\\u0024\\u0024", actual);

		actual = StateMapUtil.encodeDotAndDollar(".a.1..#..");
		assertEquals("\\u002ea\\u002e1\\u002e\\u002e#\\u002e\\u002e", actual);

		assertNull(StateMapUtil.encodeDotAndDollar(null));
	}

	@Test
	@DisplayName("Method decodeDotAndDollar test")
	void testDecodeDotAndDollar() {
		String actual = StateMapUtil.decodeDotAndDollar("a1b#");
		assertEquals("a1b#", actual);

		actual = StateMapUtil.decodeDotAndDollar("\\u0024\\u0024\\u0024a\\u00241\\u0024\\u0024#\\u0024\\u0024");
		assertEquals("$$$a$1$$#$$", actual);

		actual = StateMapUtil.decodeDotAndDollar("\\u002ea\\u002e1\\u002e\\u002e#\\u002e\\u002e");
		assertEquals(".a.1..#..", actual);

		assertNull(StateMapUtil.decodeDotAndDollar(null));
	}
}

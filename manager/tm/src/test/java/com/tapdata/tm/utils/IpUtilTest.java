package com.tapdata.tm.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author jackin
 * @Description
 * @date 2024/3/18 19:56
 **/
public class IpUtilTest {

		@Test
		public void testIpv4Check(){
				final String result = IpUtil.check("2001:0db8:85a3:0000:0000:8a2e:0370:7334");
				Assertions.assertEquals("IPv6", result);
		}

		@Test
		public void testIpv6Check(){
				final String result = IpUtil.check("192.0.2.1");
				Assertions.assertEquals("IPv4", result);
		}

		@Test
		public void testInvalidCheck(){
				final String invalidIpv4 = IpUtil.check("192.0.2.");
				Assertions.assertNull(invalidIpv4);

				final String invalidIpv6 = IpUtil.check("2001:0db8:85a3:0000:0000:8a2e:0370:733444");
				Assertions.assertNull(invalidIpv6);

				final String invalidStr = IpUtil.check("invalidString");
				Assertions.assertNull(invalidStr);
		}
}

package io.tapdata.entity.jwt;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.tapdata.pdk.core.utils.JWTUtils;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;
import static org.junit.jupiter.api.Assertions.*;

public class JWTTest {
	@Test
	public void test() {
		String key = "asdfFSDJKFHKLASHJDKQJWKJehrklHDFJKSMhkj3h24jkhhJKASDH723ty4jkhasdkdfjhaksjdfjfhJDJKLHSAfadsf";
		String token = JWTUtils.createToken(key, map(entry("aaa", "bbb"), entry("ccc", 111)), 1000L);
		assertNotNull(token);
		Map<String, Object> claims = JWTUtils.getClaims(key, token);
		assertNotNull(claims);
		assertEquals("bbb", claims.get("aaa"));
		assertEquals(111, claims.get("ccc"));


		token = JWTUtils.createToken(key, map(entry("aaa", "bbb"), entry("ccc", 111)), 0L);
		assertNotNull(token);
		try {
			claims = JWTUtils.getClaims(key, token);
			fail();
		} catch (ExpiredJwtException e) {
		} catch (Throwable throwable) {
			fail();
		}
		claims = JWTUtils.getClaimsIgnoreExpire(key, token);
		assertNotNull(claims);
		assertEquals("bbb", claims.get("aaa"));
		assertEquals(111, claims.get("ccc"));
	}
}

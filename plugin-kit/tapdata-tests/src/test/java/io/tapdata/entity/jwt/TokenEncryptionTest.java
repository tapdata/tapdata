package io.tapdata.entity.jwt;

import io.jsonwebtoken.ExpiredJwtException;
import io.tapdata.modules.api.net.entity.SubscribeToken;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.JWTUtils;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Base64;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;
import static org.junit.jupiter.api.Assertions.*;

public class TokenEncryptionTest {
	@Test
	public void tokenWithJWT() {
		String key = "asdfFSDJKFHKLASHJDKQJWKJehrklHDFJKSMhkj3h24jkhhJKASDH723ty4jkhasdkdfjhaksjdfjfhJDJKLHSAfadsf";
		String token = JWTUtils.createToken(key, map(entry("aaa", "bbb"), entry("ccc", 111)), 10000L);
		assertNotNull(token);
		Map<String, Object> claims = JWTUtils.getClaims(key, token);
		assertNotNull(claims);
		assertEquals("bbb", claims.get("aaa"));
		assertEquals(111, claims.get("ccc"));


		token = JWTUtils.createToken(key, map(entry("aaa", "bbb"), entry("ccc", 111)), 0L);
		System.out.println("token length " + token.length());
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

	@Test
	public void tokenWithRC4() throws Exception {
		SubscribeToken subscribeToken = new SubscribeToken();
		subscribeToken.setService("engine");
		subscribeToken.setSubscribeId("source#632e6df6664f9328b8bb633c");
		subscribeToken.setExpireAt((long) Integer.MAX_VALUE);

		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		subscribeToken.to(byteArrayOutputStream);
		byte[] data = byteArrayOutputStream.toByteArray();
		String key = "alksjfklasjflkasdjflkasjdlfkjsewkrkjnfsadjfilj";
		byte[] encryptedData = CommonUtils.encryptWithRC4(data, key	);
		String token = new String(Base64.getEncoder().encode(encryptedData));
		assertNotNull(token);
		String old = "eyJhbGciOiJIUzUxMiJ9.eyJzZXJ2aWNlIjoiZW5naW5lIiwic3Vic2NyaWJlSWQiOiJzb3VyY2UjNjMyZTZkZjY2NjRmOTMyOGI4YmI2MzNjIiwiZXhwIjoxNzY0MjQzNzA3LCJpYXQiOjE2NjQyNDM3MDd9.Ox6CKO60i3bJHFRHIObt_rGVvdmsyAQzJ_0qxMesV992vjaFWsfW_4zmBPIN_qeSHgtCEkFpbtolFkELHSaYQQ";
		System.out.println("token " + token + " length " + token.length() + " old length " + old.length());

		byte[] decryptedData = CommonUtils.decryptWithRC4(Base64.getDecoder().decode(token), key);
		assertNotNull(decryptedData);

		SubscribeToken decryptedToken = new SubscribeToken();
		decryptedToken.from(new ByteArrayInputStream(decryptedData));
		assertEquals("engine", decryptedToken.getService());
		assertEquals("source#632e6df6664f9328b8bb633c", decryptedToken.getSubscribeId());
		assertEquals(Integer.MAX_VALUE, decryptedToken.getExpireAt());
	}


}

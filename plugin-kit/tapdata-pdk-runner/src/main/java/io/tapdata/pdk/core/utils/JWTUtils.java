package io.tapdata.pdk.core.utils;

import io.jsonwebtoken.*;

import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;
import java.security.Key;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class JWTUtils {

    public static String createToken(String key, Map<String, Object> claims, Long expireTime) {
        Key signingKey = getKey(key);

        Map<String, Object> map = new HashMap<>();
        map.put("alg", "HS256");
        map.put("typ", "JWT");

//        Jwts.builder().setId(id)
//                .setIssuedAt(now)
//                .setSubject(subject)
//                .setIssuer(issuer)
//                .signWith(signatureAlgorithm, signingKey)

        JwtBuilder jwtBuilder = Jwts.builder();
        if (claims != null) {
            jwtBuilder.setClaims(claims);
        }
        if (expireTime != null) {
            jwtBuilder.setExpiration(new Date(System.currentTimeMillis() + expireTime));
        }
        return jwtBuilder
                .setIssuedAt(new Date())
                .signWith(signingKey)
                .compact();
    }

    public static Map<String, Object> getClaims(String key, String token) {
        Key signingKey = getKey(key);

        return (Map<String, Object>) Jwts.parserBuilder().setSigningKey(signingKey).build().parse(token).getBody();
//        return Jwts.parser().setSigningKey(key).parseClaimsJws(token).getBody();
    }

    private static Key getKey(String key) {
        SignatureAlgorithm signatureAlgorithm = SignatureAlgorithm.HS256;
        byte[] apiKeySecretBytes = DatatypeConverter.parseBase64Binary(key);
        Key signingKey = new SecretKeySpec(apiKeySecretBytes, signatureAlgorithm.getJcaName());
        return signingKey;
    }

    public static Map<String, Object> getClaimsIgnoreExpire(String key, String token) {
        Key signingKey = getKey(key);
        try {
            return (Map<String, Object>) Jwts.parserBuilder().setSigningKey(signingKey).build().parse(token).getBody();
        } catch (ExpiredJwtException e) {
            return e.getClaims();
        }
    }
}
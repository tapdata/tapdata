package com.tapdata.tm.oauth;

import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.IOException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.UUID;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/12/25 上午11:20
 */
public class TestLoadPEM {

    @Test
    public void testLoadPEM() throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        File file = ResourceUtils.getFile("classpath:oauth/private.pem");
        String privateKeyData = FileUtils.readFileToString(file);
        file = ResourceUtils.getFile("classpath:oauth/public.pem");
        String publicKeyData = FileUtils.readFileToString(file);

        privateKeyData = privateKeyData
                .replace("-----BEGIN PRIVATE KEY-----", "")
                .replaceAll(System.lineSeparator(), "")
                .replace("-----END PRIVATE KEY-----", "");

        publicKeyData = publicKeyData
                .replace("-----BEGIN PUBLIC KEY-----", "")
                .replaceAll(System.lineSeparator(), "")
                .replace("-----END PUBLIC KEY-----", "");

        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        PKCS8EncodedKeySpec encodedPrivateKeySpec = new PKCS8EncodedKeySpec(Base64.getDecoder().decode(privateKeyData));
        X509EncodedKeySpec encodedPublicKeySpec = new X509EncodedKeySpec(Base64.getDecoder().decode(publicKeyData));

        RSAPrivateKey privateKey = (RSAPrivateKey) keyFactory.generatePrivate(encodedPrivateKeySpec);
        RSAPublicKey publicKey = (RSAPublicKey) keyFactory.generatePublic(encodedPublicKeySpec) ;

        RSAKey rsaKey = new RSAKey.Builder(publicKey)
                .privateKey(privateKey)
                .keyID(UUID.randomUUID().toString())
                .build();
        JWKSet jwkSet = new JWKSet(rsaKey);

        System.out.println(jwkSet.toJSONObject());
    }
}

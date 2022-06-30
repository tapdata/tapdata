/**
 * @title: RC4Util
 * @description:
 * @author lk
 * @date 2021/12/1
 */
package com.tapdata.tm.utils;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Arrays;
import java.util.Base64;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

public class RC4Util {

	private static final String ALGORITHM = "RC4";
	/** OpenSSL's magic initial bytes. */
	private static final String SALTED_STR = "Salted__";
	private static final byte[] SALTED_MAGIC = SALTED_STR.getBytes(US_ASCII);

	/**
	 *
	 * @param password  The password / key to encrypt with.
	 * @param plaintext      The data to encrypt
	 * @return  A base64 encoded string containing the encrypted data.
	 */
	public static String encrypt(String password, String plaintext) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidAlgorithmParameterException, InvalidKeyException, BadPaddingException, IllegalBlockSizeException {
		final byte[] pass = password.getBytes(US_ASCII);
		final byte[] salt = (new SecureRandom()).generateSeed(8);
		final byte[] inBytes = plaintext.getBytes(UTF_8);

		final byte[] passAndSalt = arrayConcat(pass, salt);
		byte[] hash = new byte[0];
		byte[] keyAndIv = new byte[0];
		for (int i = 0; i < 3 && keyAndIv.length < 48; i++) {
			final byte[] hashData = arrayConcat(hash, passAndSalt);
			final MessageDigest md = MessageDigest.getInstance("MD5");
			hash = md.digest(hashData);
			keyAndIv = arrayConcat(keyAndIv, hash);
		}

		final byte[] keyValue = Arrays.copyOfRange(keyAndIv, 0, 32);
		final byte[] iv = Arrays.copyOfRange(keyAndIv, 32, 48);
		final SecretKeySpec key = new SecretKeySpec(keyValue, ALGORITHM);

		final Cipher cipher = Cipher.getInstance(ALGORITHM);
		cipher.init(Cipher.ENCRYPT_MODE, key, (AlgorithmParameterSpec)null);
		byte[] data = cipher.doFinal(inBytes);
		data =  arrayConcat(arrayConcat(SALTED_MAGIC, salt), data);
		return Base64.getEncoder().encodeToString( data );
	}

	/**
	 * http://stackoverflow.com/questions/32508961/java-equivalent-of-an-openssl-aes-cbc-encryption  for what looks like a useful answer.  The not-yet-commons-ssl also has an implementation
	 * @param password key
	 * @param ciphertext The encrypted data
	 * @return String
	 */
	public static String decrypt(String password, String ciphertext) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidAlgorithmParameterException, InvalidKeyException, BadPaddingException, IllegalBlockSizeException {
		final byte[] pass = password.getBytes(US_ASCII);

		final byte[] inBytes = Base64.getDecoder().decode(ciphertext);

		final byte[] shouldBeMagic = Arrays.copyOfRange(inBytes, 0, SALTED_MAGIC.length);
		if (!Arrays.equals(shouldBeMagic, SALTED_MAGIC)) {
			throw new IllegalArgumentException("Initial bytes from input do not match OpenSSL SALTED_MAGIC salt value.");
		}

		final byte[] salt = Arrays.copyOfRange(inBytes, SALTED_MAGIC.length, SALTED_MAGIC.length + 8);

		final byte[] passAndSalt = arrayConcat(pass, salt);

		byte[] hash = new byte[0];
		byte[] keyAndIv = new byte[0];
		for (int i = 0; i < 3 && keyAndIv.length < 48; i++) {
			final byte[] hashData = arrayConcat(hash, passAndSalt);
			final MessageDigest md = MessageDigest.getInstance("MD5");
			hash = md.digest(hashData);
			keyAndIv = arrayConcat(keyAndIv, hash);
		}

		final byte[] keyValue = Arrays.copyOfRange(keyAndIv, 0, 32);
		final SecretKeySpec key = new SecretKeySpec(keyValue, ALGORITHM);

//		final byte[] iv = Arrays.copyOfRange(keyAndIv, 32, 48);

		final Cipher cipher = Cipher.getInstance(ALGORITHM);
		cipher.init(Cipher.DECRYPT_MODE, key, (AlgorithmParameterSpec)null);
		final byte[] clear = cipher.doFinal(inBytes, 16, inBytes.length - 16);
		return new String(clear, UTF_8);
	}


	private static byte[] arrayConcat(final byte[] a, final byte[] b) {
		final byte[] c = new byte[a.length + b.length];
		System.arraycopy(a, 0, c, 0, a.length);
		System.arraycopy(b, 0, c, a.length, b.length);
		return c;
	}
}

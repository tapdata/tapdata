package com.tapdata.processor.util;

import com.google.common.collect.Lists;
import com.tapdata.constant.MapUtil;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.json.XML;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Util {

	public static String strToBase64(String str) {
		if (StringUtils.isEmpty(str)) {
			return null;
		}
		Base64.Encoder encoder = Base64.getEncoder();
		return encoder.encodeToString(str.getBytes());
	}

	public static String base64ToStr(String str) {
		if (StringUtils.isEmpty(str)) {
			return null;
		}
		Base64.Decoder decoder = Base64.getDecoder();
		return new String(decoder.decode(str.getBytes()));
	}

	public static byte toByte(short s) {
		if (s > 0xff || s < 0) {
			throw new IllegalArgumentException(s + ": Value must be between 0-255");
		}
		return (byte) s;
	}

	public static byte toByte(char c) {
		return (byte) c;
	}

	public static byte[] toBytes(String str) {
		return str.getBytes();
	}

	public static byte[] toBytes(String str, int length) {
		if (str.length() > length) {
			throw new IllegalArgumentException(str.length() + "must less or equals" + length + "");
		}
		if (str.length() == length) {
			return str.getBytes();
		}
		byte[] bytes = new byte[length];

		arraycopy(str.getBytes(), 0, bytes, length - str.length(), str.length());

		return bytes;
	}

	public static byte[] toWord(int i) {
		if (i > 0xffff || i < 0) {
			throw new IllegalArgumentException(i + ": Value must be between 0-65535");
		}
		byte[] word = new byte[2];
		word[1] = (byte) i;
		word[0] = (byte) (i >> 8);
		return word;
	}

	public static byte[] toDword(long l) {
		if (l > 0xffffffffL || l < 0) {
			throw new IllegalArgumentException(l + ": Value must be between 0-4294967295");
		}
		byte[] dword = new byte[4];
		dword[3] = (byte) l;
		dword[2] = (byte) (l >> 8);
		dword[1] = (byte) (l >> 16);
		dword[0] = (byte) (l >> 24);
		return dword;
	}

	/**
	 * 创建数组对象
	 *
	 * @param size
	 * @return
	 */
	public static byte[] newBytes(int size) {
		return new byte[size];
	}

	/**
	 * 将src设置到dest的指定位置中
	 *
	 * @param src
	 * @param srcPos
	 * @param dest
	 * @param destPos
	 * @param length
	 */
	public static void arraycopy(Object src, int srcPos, Object dest, int destPos, int length) {
		System.arraycopy(src, srcPos, dest, destPos, length);
	}

	/**
	 * 获取校验码
	 *
	 * @param bytes
	 * @return
	 */
	public static byte getCheckCode(byte[] bytes) {
		byte code = bytes[0];
		for (int i = 1; i < bytes.length; i++) {
			code ^= bytes[i];
		}
		return code;
	}

	public static int getBytesLength(byte[] bytes) {
		return bytes.length;
	}

	/**
	 * 转换为hex格式显示
	 *
	 * @param bytes
	 * @return
	 */
	public static String toHexString(byte[] bytes) {
		StringBuilder sb = new StringBuilder();
		for (byte aByte : bytes) {
			String hexStr = Integer.toHexString(aByte);
			if (hexStr.length() == 1) {
				sb.append(0);
			}
			if (hexStr.length() > 2) {
				sb.append(hexStr.substring(hexStr.length() - 2));
			} else {
				sb.append(hexStr);
			}
		}
		return sb.toString();
	}

	public static List<Map<String, Object>> unwind(Map<String, Object> record, String... keys) throws Exception {

		List<Map<String, Object>> unwindList = Lists.newArrayList(record);
		for (String key : keys) {
			unwindList = unwind(unwindList, key);
		}
		return unwindList;
	}

	public static List<Map<String, Object>> unwind(List<Map<String, Object>> recordList, String key) throws Exception {
		List<Map<String, Object>> unwindResult = new ArrayList<>();
		for (Map<String, Object> record : recordList) {
			unwindResult.addAll(unwind(record, key));
		}
		return unwindResult;
	}

	/**
	 * 平铺数组
	 *
	 * @param record
	 * @param key
	 * @return
	 */
	public static List<Map<String, Object>> unwind(Map<String, Object> record, String key) throws Exception {

		final Object valueObj = MapUtil.getValueByKey(record, key);
		if (!(valueObj instanceof List)) {
			return Lists.newArrayList(record);
		}
		List<Map<String, Object>> resultList = new ArrayList<>();
		MapUtil.removeKey(record, key);
		for (Object obj : (List) valueObj) {
			Map<String, Object> newMap = new HashMap<>();
			MapUtil.copyToNewMap(record, newMap);
			MapUtil.putValueInMap(newMap, key, obj);
			resultList.add(newMap);
		}

		return resultList;
	}

	@SneakyThrows
	public static void main(String[] args) {
		Path path = Paths.get("/Users/zerohyuan/Downloads/INV");
		String xml = String.join("\n", Files.readAllLines(path));
		System.out.println(xml);
		Map<String, Object> record = XML.toJSONObject(xml).toMap();

		List<Map<String, Object>> mapList = unwind(record, "MSG.DAT.Flight.Leg.CabinGroup.Cabin");
		for (Map<String, Object> item : mapList) {
//      System.out.println(item.getClass());
			System.out.println(MapUtil.getValueByKey(item, "MSG.DAT").hashCode());
		}

		List<Map<String, Object>> resultList = new ArrayList<>();
		for (Map<String, Object> map : mapList) {
			resultList.addAll(unwind(map, "MSG.DAT.Flight.Leg.CabinGroup.Cabin.ClassOfService"));
		}
		Set<Object> collect = resultList.stream().map(map -> MapUtil.getValueByKey(map, "MSG.DAT.Flight.Leg.CabinGroup.Cabin.ClassOfService")).collect(Collectors.toSet());
		System.out.println(collect);
//    byte[] bytes = toBytes("##", 12);
//    System.out.println(toHexString(bytes));
//
//    byte[] bytes1 = newBytes(2);
//    System.out.println(0xfe);
//    bytes1[0] = toByte((short) 0x05);
//    bytes1[1] = toByte((short) 0xfe);
//
//    System.out.println(toHexString(bytes1));

		//000000000000000000002323
	}

}

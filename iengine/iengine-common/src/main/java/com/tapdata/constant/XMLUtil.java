package com.tapdata.constant;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class XMLUtil {

	public static Document map2xml(Map<String, Object> map, String rootName) {
		Document doc = null;
		if (MapUtils.isNotEmpty(map) && StringUtils.isNotBlank(rootName)) {
			doc = DocumentHelper.createDocument();
			Element root = DocumentHelper.createElement(rootName);
			doc.add(root);
			map2xml(map, root);
		}
		return doc;
	}

	public static Document map2xml(Map<String, Object> map) {
		Iterator<Map.Entry<String, Object>> entries = map.entrySet().iterator();
		if (entries.hasNext()) {
			Map.Entry<String, Object> entry = entries.next();
			Document doc = DocumentHelper.createDocument();
			Element root = DocumentHelper.createElement(entry.getKey());
			doc.add(root);
			map2xml((Map) entry.getValue(), root);
			return doc;
		}
		return null;
	}

	private static Element map2xml(Map<String, Object> map, Element body) {
		Iterator<Map.Entry<String, Object>> entries = map.entrySet().iterator();
		while (entries.hasNext()) {
			Map.Entry<String, Object> entry = entries.next();
			String key = entry.getKey();
			Object value = entry.getValue();
			value = value == null ? "" : value;
			if (key.startsWith("@")) {
				body.addAttribute(key.substring(1, key.length()), value.toString());
			} else if (key.equals("#text")) {
				body.setText(value.toString());
			} else {
				if (value instanceof java.util.List) {
					List list = (List) value;
					Object obj;
					for (int i = 0; i < list.size(); i++) {
						obj = list.get(i);
						if (obj instanceof java.util.Map) {
							Element subElement = body.addElement(key);
							map2xml((Map) list.get(i), subElement);
						} else {
							body.addElement(key).setText((String) list.get(i));
						}
					}
				} else if (value instanceof java.util.Map) {
					Element subElement = body.addElement(key);
					map2xml((Map) value, subElement);
				} else {
					body.addElement(key).setText(value.toString());
				}
			}
//            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
		}
		return body;
	}

	public static String formatXml(String xmlStr) throws DocumentException, IOException {
		Document document = DocumentHelper.parseText(xmlStr);
		return formatXml(document);
	}

	public static String formatXml(Document document) throws IOException {
		OutputFormat format = OutputFormat.createPrettyPrint();
		StringWriter writer = new StringWriter();
		XMLWriter xmlWriter = new XMLWriter(writer, format);
		xmlWriter.write(document);
		xmlWriter.close();
		return writer.toString();
	}

	public static void main(String[] args) throws IOException {
		Map<String, Object> map = new HashMap<>();
		Map<String, Object> subMap = new HashMap<>();
		List<Map> list = new ArrayList<>();

		for (int i = 0; i < 10; i++) {
			subMap = new HashMap<>();
			subMap.put("name", "name" + i);
			subMap.put("age", i + 1);
			list.add(subMap);
		}

		map.put("root", list);

//        String jsonString = "{\"_id\":{\"$oid\":\"5c77479c47da04d4586b3def\"},\"name\":\"json source\",\"connection_type\":\"source\",\"database_type\":\"gridfs\"}";
//
//        map = JSONUtil.json2Map(jsonString);
//
//        System.out.println(map.size());


		Document doc = map2xml(map, "student");
		System.out.println(doc.asXML());
		System.out.println(formatXml(doc));
	}
}

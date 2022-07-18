package com.tapdata.processor;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class DataRulesProcessorTest {

	@Test
	public void nullableTest() {
		assertTrue("failure - condition=true, value='test', result should be true", DataRulesProcessor.nullable("test", true));
		assertTrue("failure - condition=false, value=null, result should be true", DataRulesProcessor.nullable("test", false));
		assertFalse("failure - condition=false, value=null, result should be false", DataRulesProcessor.nullable(null, false));
		assertTrue("failure - condition=null, value='test', result should be true", DataRulesProcessor.nullable("test", null));
	}

	@Test
	public void typeTest() {
		assertTrue("failure - should be true", DataRulesProcessor.type("test", "string"));
		assertTrue("failure - should be true", DataRulesProcessor.type(Short.valueOf("1"), "short"));
		assertTrue("failure - should be true", DataRulesProcessor.type(Integer.valueOf("1"), "int"));
		assertTrue("failure - should be true", DataRulesProcessor.type(Long.valueOf("1"), "long"));
		assertTrue("failure - should be true", DataRulesProcessor.type(Double.valueOf("1"), "double"));
		assertTrue("failure - should be true", DataRulesProcessor.type(Float.valueOf("1"), "float"));
		assertTrue("failure - should be true", DataRulesProcessor.type(Boolean.valueOf("1"), "boolean"));
		assertTrue("failure - should be true", DataRulesProcessor.type(null, "string"));
		assertTrue("failure - should be true", DataRulesProcessor.type("test", "test"));
		assertFalse("failure - should be false", DataRulesProcessor.type(Integer.valueOf("1"), "string"));
		assertFalse("failure - should be false", DataRulesProcessor.type("test", "short"));
		assertFalse("failure - should be false", DataRulesProcessor.type("test", "int"));
		assertFalse("failure - should be false", DataRulesProcessor.type("test", "long"));
		assertFalse("failure - should be false", DataRulesProcessor.type("test", "double"));
		assertFalse("failure - should be false", DataRulesProcessor.type("test", "float"));
		assertFalse("failure - should be false", DataRulesProcessor.type("test", "boolean"));
	}

	@Test
	public void rangeTest() {
		Map<String, Object> con = new HashMap<String, Object>() {{
			put("gte", 1);
			put("lte", 10);
		}};

		assertTrue("failure - should be true", DataRulesProcessor.range(1, con));
		assertTrue("failure - should be true", DataRulesProcessor.range(10, con));
		assertTrue("failure - should be true", DataRulesProcessor.range(2.3, con));
		assertFalse("failure - should be false", DataRulesProcessor.range(11, con));

		con = new HashMap<String, Object>() {{
			put("gt", "1");
			put("lt", "10");
		}};

		assertTrue("failure - should be true", DataRulesProcessor.range(2, con));
		assertFalse("failure - should be false", DataRulesProcessor.range(1, con));

		con = new HashMap<String, Object>() {{
			put("none", "");
			put("gte", 10);
		}};

		assertTrue("failure - should be true", DataRulesProcessor.range(10, con));
		assertTrue("failure - should be true", DataRulesProcessor.range(1000000, con));
		assertTrue("failure - should be true", DataRulesProcessor.range(1000000.123, con));
		assertFalse("failure - should be false", DataRulesProcessor.range(1, con));
	}

	@Test
	public void enumTest() {
		List<Object> con = new ArrayList<>();
		con.add("a");
		con.add("b");

		assertTrue("failure - should be true", DataRulesProcessor.enumChk("a", con));
		assertFalse("failure - should be false", DataRulesProcessor.enumChk("c", con));
	}

	@Test
	public void regexTest() {
		String con = "^user\\d{3}$";

		assertTrue("failure - should be true", DataRulesProcessor.regexChk("user001", con));
		assertFalse("failure - should be false", DataRulesProcessor.regexChk("user01", con));
		assertFalse("failure - should be false", DataRulesProcessor.regexChk("001", con));
	}
}

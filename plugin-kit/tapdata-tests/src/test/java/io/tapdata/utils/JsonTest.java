package io.tapdata.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.TypeHolder;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class JsonTest {
	@Test
	public void test() throws JsonProcessingException {
		String json = "[{\n" +
				"    \"lastUpdate\": 1666579789867,\n" +
				"    \"indexList\": null,\n" +
				"    \"id\": \"BA_LICENSE\",\n" +
				"    \"name\": \"BA_LICENSE\",\n" +
				"    \"storageEngine\": null,\n" +
				"    \"charset\": null,\n" +
				"    \"comment\": null,\n" +
				"    \"pdkId\": null,\n" +
				"    \"pdkGroup\": null,\n" +
				"    \"pdkVersion\": null,\n" +
				"    \"maxPos\": 19,\n" +
				"    \"maxPKPos\": 1,\n" +
				"    \"nameFieldMap\": {\"_id\" : {\"dataType\": \"OBJECT_ID\"}," +

				" \"lockUntil\": {\n" +
				"        \"dataType\": \"DATE_TIME\",\n" +
				"        \"nullable\": true,\n" +
				"        \"name\": \"lockUntil\",\n" +
				"        \"partitionKeyPos\": null,\n" +
				"        \"pos\": 4,\n" +
				"        \"primaryKeyPos\": null,\n" +
				"        \"foreignKeyTable\": null,\n" +
				"        \"foreignKeyField\": null,\n" +
				"        \"defaultValue\": null,\n" +
				"        \"autoInc\": false,\n" +
				"        \"autoIncStartValue\": null,\n" +
				"        \"check\": null,\n" +
				"        \"comment\": null,\n" +
				"        \"constraint\": null,\n" +
				"        \"tapType\": {\n" +
				"            \"type\": 1,\n" +
//				"            \"withTimeZone\": null,\n" +
//				"            \"bytes\": null,\n" +
				"            \"min\": -30610224000.001,\n" +
				"            \"max\": 253402300799.999,\n" +
//				"\"min\":\"-1000000000-01-01T00:00:00Z\",\"max\":\"+1000000000-12-31T23:59:59.999999999Z\"" +
//				"            \"fraction\": 3,\n" +
//				"            \"defaultFraction\": 3\n" +
				"        },\n" +
				"        \"primaryKey\": false,\n" +
				"        \"partitionKey\": false\n" +
				"    }" +

				"}\n" +
				"}]";

		JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);
		TapTable table = table("aaaa").add(field("field1", "varchar").tapType(tapDateTime().min(Instant.MIN).max(Instant.MAX)));
		String tableJson = jsonParser.toJson(table);
		TapTable tapTable = jsonParser.fromJson(tableJson, TapTable.class);
		assertEquals("+1000000000-12-31T23:59:59.999999999Z", ((TapDateTime)tapTable.getNameFieldMap().get("field1").getTapType()).getMax().toString());
		assertEquals("-1000000000-01-01T00:00:00Z", ((TapDateTime)tapTable.getNameFieldMap().get("field1").getTapType()).getMin().toString());
		List<TapTable> list = jsonParser.fromJson(json, new TypeHolder<List<TapTable>>(){});
		assertNotNull(list);
		assertEquals(1, list.size());
		assertEquals("9999-12-31T23:59:59.999Z", ((TapDateTime)list.get(0).getNameFieldMap().get("lockUntil").getTapType()).getMax().toString());
		assertEquals("1000-01-01T00:00:00.001Z", ((TapDateTime)list.get(0).getNameFieldMap().get("lockUntil").getTapType()).getMin().toString());
	}
}

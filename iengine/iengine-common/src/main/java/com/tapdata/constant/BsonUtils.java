package com.tapdata.constant;

import com.mongodb.DBRef;
import org.bson.BsonValue;
import org.bson.Document;

import java.util.Date;

/**
 * @author samuel
 * @Description
 * @create 2021-06-23 19:45
 **/
public class BsonUtils {

	public static Object toJavaType(BsonValue value) throws Exception {
		switch (value.getBsonType()) {
			case INT32:
				return value.asInt32().getValue();
			case INT64:
				return value.asInt64().getValue();
			case STRING:
				return value.asString().getValue();
			case DECIMAL128:
				return value.asDecimal128().doubleValue();
			case DOUBLE:
				return value.asDouble().getValue();
			case BOOLEAN:
				return value.asBoolean().getValue();
			case OBJECT_ID:
				return value.asObjectId().getValue();
			case DB_POINTER:
				return new DBRef(value.asDBPointer().getNamespace(), value.asDBPointer().getId());
			case BINARY:
				return value.asBinary().getData();
			case DATE_TIME:
				return new Date(value.asDateTime().getValue());
			case SYMBOL:
				return value.asSymbol().getSymbol();
			case ARRAY:
				return value.asArray().toArray();
			case DOCUMENT:
				return Document.parse(value.asDocument().toJson());
			default:
				throw new Exception("Unsupported bsonvalue type: " + value.getBsonType() + ", value: " + value);
		}
	}
}

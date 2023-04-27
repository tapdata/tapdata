package com.tapdata.constant;

import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.RelateDataBaseTable;
import com.tapdata.entity.RelateDatabaseField;
import com.tapdata.entity.TapLog;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.data.TimeConversions.DateConversion;
import org.apache.avro.data.TimeConversions.TimeMicrosConversion;
import org.apache.avro.data.TimeConversions.TimeMillisConversion;
import org.apache.avro.data.TimeConversions.TimestampMicrosConversion;
import org.apache.avro.data.TimeConversions.TimestampMillisConversion;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvroUtil {

	public final static Schema DATE_SCHEMA = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
	public final static Schema TIME_MILLIS_SCHEMA = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
	public final static Schema TIME_MICROS_SCHEMA = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
	public final static Schema TIMESTAMP_MILLIS_SCHEMA = LogicalTypes.timestampMillis()
			.addToSchema(Schema.create(Schema.Type.LONG));
	public final static Schema TIMESTAMP_MICROS_SCHEMA = LogicalTypes.timestampMicros()
			.addToSchema(Schema.create(Schema.Type.LONG));

	public final static DateConversion DATE_CONVERSION = new DateConversion();
	public final static TimeMillisConversion TIME_MILLIS_CONVERSION = new TimeMillisConversion();
	public final static TimeMicrosConversion TIME_MICROS_CONVERSION = new TimeMicrosConversion();
	public final static TimestampMillisConversion TIMESTAMP_MILLIS_CONVERSION = new TimestampMillisConversion();
	public final static TimestampMicrosConversion TIMESTAMP_MICROS_CONVERSION = new TimestampMicrosConversion();

	public final static String FILE_WRITER = "file";
	public final static String JSON_ENCODER = "json";
	public final static String BINARY_ENCODER = "binary";


	private List<MessageEntity> msgs;

	public void setMsgs(List<MessageEntity> msgs) {
		this.msgs = msgs;
	}

	public AvroUtil() {
	}

	public Schema getAvroSchemaByTablename(
			String namespace
	) {
		Map<String, Object> data = new HashMap<>();

		buildSchemaMap(data);

		String tableName = msgs.get(0).getTableName();

		namespace = getNameSpace(namespace, tableName);
		Schema schema = buildSchemaFromData(data, namespace, tableName);

		if (schema != null) {
			return schema;
		} else {
			return null;
		}
	}

	private void buildSchemaMap(Map<String, Object> data) {
		if (data != null && CollectionUtils.isNotEmpty(msgs)) {
            /*data.putAll(msgs.get(0).getAfter());

            for (Map.Entry<String, Object> entry : data.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (value == null) {
                    for (MessageEntity msg : msgs) {
                        Map<String, Object> after = msg.getAfter();
                        if (after.get(key) != null) {
                            data.put(key, after.get(key));
                            break;
                        }
                    }
                }
            }*/

			for (MessageEntity msg : msgs) {
				Map<String, Object> after = msg.getAfter();
				for (Map.Entry<String, Object> entry : after.entrySet()) {
					String key = entry.getKey();
					Object value = entry.getValue();
					if (data.containsKey(key)) {
						if (data.get(key) == null
								&& value != null) {
							data.put(key, value);
						}
					} else {
						data.put(key, value);
					}
				}
			}
		}
	}

	private static Schema buildSchemaFromData(
			Map<String, Object> data,
			String namespace,
			String tableName
	) {
		if (MapUtils.isNotEmpty(data)
				&& StringUtils.isNotBlank(namespace)) {
			SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record(tableName).namespace(namespace).fields();

			for (Map.Entry<String, Object> entry : data.entrySet()) {
				String fieldName = entry.getKey();
				Object value = entry.getValue();

				javaType2AvroType(fields, fieldName, value);
			}

			return fields.endRecord();
		} else {
			return null;
		}
	}

	private static void javaType2AvroType(
			SchemaBuilder.FieldAssembler fields,
			String fieldName,
			Object value
	) {
		SchemaBuilder.FieldBuilder name = fields.name(fieldName);
		SchemaBuilder.FieldTypeBuilder type = name.type();
		SchemaBuilder.BaseFieldTypeBuilder base = type.nullable();
		if (value instanceof String
				|| value instanceof java.sql.Clob
				|| value instanceof java.sql.Blob
				|| value instanceof java.sql.NClob
				|| value instanceof ObjectId) {
			base.stringType().noDefault();
		} else if (value instanceof Integer) {
			base.intType().noDefault();
		} else if (value instanceof Long) {
			base.longType().noDefault();
		} else if (value instanceof Float) {
			base.floatType().noDefault();
		} else if (value instanceof Double
				|| value instanceof BigDecimal) {
			base.doubleType().noDefault();
		} else if (value instanceof byte[]) {
			base.bytesType().noDefault();
		} else if (value instanceof java.sql.Date) {
			name.type(DATE_SCHEMA).noDefault();
		} else if (value instanceof java.sql.Time) {
			name.type(TIME_MICROS_SCHEMA).noDefault();
		} else if (value instanceof java.sql.Timestamp
				|| value instanceof java.util.Date) {
			name.type(TIMESTAMP_MICROS_SCHEMA).noDefault();
		} else if (value == null) {
			name.type().nullType().nullDefault();
		}
	}

	public static byte[] messages2AvroBytes(
			List<MessageEntity> messages,
			String encoderType,
			Schema schema,
			Logger logger
	) throws NullPointerException {
		byte[] bytes = new byte[32];

		if (schema == null) {
			throw new NullPointerException("Avro schema null.");
		}

		if (messages == null) {
			throw new NullPointerException("List messages null.");
		}

		OutputStream outputStream = null;
		Encoder encoder = null;
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = null;
		try {
			outputStream = new ByteArrayOutputStream();
			encoderType = handleEncoderType(encoderType);
			if (encoderType.equalsIgnoreCase(FILE_WRITER)) {
				dataFileWriter = new DataFileWriter<>(datumWriter);
				try {
					dataFileWriter.create(schema, outputStream);
				} catch (IOException e) {
					logger.error(TapLog.TRAN_ERROR_0019.getMsg(), e.getMessage(), e);
				}
			} else {
				try {
					encoder = initEncoder(encoderType, schema, outputStream);
				} catch (IOException e) {
					logger.error(TapLog.TRAN_ERROR_0016.getMsg(), e.getMessage(), e);
					return bytes;
				}
			}

			for (MessageEntity message : messages) {
				Map<String, Object> after = message.getAfter();
				GenericRecord genericRecord = new GenericData.Record(schema);

				for (Map.Entry<String, Object> entry : after.entrySet()) {
					String key = entry.getKey();
					Object value = entry.getValue();

					try {
						if (value instanceof java.sql.Date) {
							genericRecord.put(key, DATE_CONVERSION.toInt(((java.sql.Date) value).toLocalDate(), schema, LogicalTypes.date()));
						} else if (value instanceof Time) {
							genericRecord.put(key, TIME_MICROS_CONVERSION.toLong(((Time) value).toLocalTime(), schema, LogicalTypes.timeMicros()));
						} else if (value instanceof Timestamp) {
							genericRecord.put(key, TIMESTAMP_MICROS_CONVERSION.toLong(((Timestamp) value).toInstant(), schema, LogicalTypes.timestampMicros()));
						} else if (value instanceof Date) {
							genericRecord.put(key, TIMESTAMP_MICROS_CONVERSION.toLong(((Date) value).toInstant(), schema, LogicalTypes.timestampMicros()));
						} else if (value instanceof ObjectId) {
							String objectId = ((ObjectId) value).toHexString();
							genericRecord.put(key, objectId);
						} else {
							genericRecord.put(key, value);
						}
					} catch (Exception e) {
						logger.warn("Failed to put value in avro data will be ignore, message: {}, field: {}, value: {}."
								, e.getMessage()
								, key
								, value);
						continue;
					}
				}

				try {
					if (encoderType.equalsIgnoreCase(FILE_WRITER)) {
						dataFileWriter.append(genericRecord);
					} else {
						datumWriter.write(genericRecord, encoder);
					}
				} catch (Exception e) {
					logger.warn(TapLog.TRAN_ERROR_0017.getMsg(), e.getMessage(), genericRecord.toString());
					continue;
				}
			}
			if (encoder != null) {
				try {
					encoder.flush();
				} catch (Exception e) {
					logger.error("Failed to flush data, message: {}.", e.getMessage());
				}
			}
			bytes = ((ByteArrayOutputStream) outputStream).toByteArray();
		} finally {
			try {
				if (encoder != null) {
					encoder.flush();
				}
			} catch (Exception e) {
				logger.error(TapLog.TRAN_ERROR_0018.getMsg(), e.getMessage(), e);
			}
			if (outputStream != null) {
				try {
					outputStream.close();
				} catch (IOException e) {
					logger.error("Failed to close outputStream, message: {}.", e.getMessage(), e);
				}
			}

			if (dataFileWriter != null) {
				try {
					dataFileWriter.close();
				} catch (Exception e) {
					// do nothing
				}
			}
		}

		return bytes;
	}

	private static String handleEncoderType(String encoderType) {
		if (StringUtils.isBlank(encoderType)
				|| (!encoderType.equalsIgnoreCase(FILE_WRITER)
				&& !encoderType.equalsIgnoreCase(JSON_ENCODER)
				&& !encoderType.equalsIgnoreCase(BINARY_ENCODER))) {
			encoderType = FILE_WRITER;
		}

		return encoderType;
	}

	private static Encoder initEncoder(String encoderType, Schema schema, OutputStream outputStream) throws IOException {
		Encoder encoder = null;
		switch (encoderType) {
			case JSON_ENCODER:
				encoder = EncoderFactory.get().jsonEncoder(schema, outputStream, true);

				break;

			case BINARY_ENCODER:
				encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

				break;
		}

		return encoder;
	}

	public static Map<String, Schema> getAvroSchemaMap(
			List<RelateDataBaseTable> relateDataBaseTables,
			String namespace,
			DatabaseTypeEnum sourceDatabaseType
	) {
		Map<String, Schema> schemaMap = new HashMap<>();

		if (CollectionUtils.isNotEmpty(relateDataBaseTables)) {
			for (RelateDataBaseTable relateDataBaseTable : relateDataBaseTables) {
				schemaMap.put(relateDataBaseTable.getTable_name(),
						getAvroSchema(
								relateDataBaseTable,
								getNameSpace(namespace, relateDataBaseTable.getTable_name()),
								sourceDatabaseType
						));
			}
		}

		return schemaMap;
	}

	private static String getNameSpace(String namespace, String tableName) throws NullPointerException {
		String finalNamespace;
		if (StringUtils.isNotBlank(namespace)) {
			finalNamespace = namespace.trim();
		} else {
			if (StringUtils.isBlank(tableName)) {
				throw new NullPointerException();
			} else {
				finalNamespace = tableName;
			}
		}

		return finalNamespace;
	}

	public static Schema getAvroSchema(RelateDataBaseTable relateDataBaseTable, String namespace, DatabaseTypeEnum sourceDatabaseType) {
		Schema schema = null;
		if (relateDataBaseTable != null) {
			SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record(relateDataBaseTable.getTable_name()).namespace(namespace).fields();
			List<RelateDatabaseField> relateDataBaseTableFields = relateDataBaseTable.getFields();

			if (CollectionUtils.isNotEmpty(relateDataBaseTableFields)) {
				for (RelateDatabaseField relateDataBaseTableField : relateDataBaseTableFields) {
					switch (sourceDatabaseType) {
						case ORACLE:
						case MYSQL:
						case MARIADB:
						case DAMENG:
						case MYSQL_PXC:
						case KUNDB:
						case ADB_MYSQL:
						case ALIYUN_MYSQL:
						case ALIYUN_MARIADB:
						case MSSQL:
						case ALIYUN_MSSQL:
						case SYBASEASE:
							jdbcType2AvroType(relateDataBaseTableField, fields, sourceDatabaseType);
							break;

						case MONGODB:
						case ALIYUN_MONGODB:

							break;

						default:
							break;
					}
				}
				schema = fields.endRecord();
			}
		}

		return schema;
	}

	private static void jdbcType2AvroType(RelateDatabaseField relateDataBaseTableField, SchemaBuilder.FieldAssembler fields, DatabaseTypeEnum sourceDatabaseType) {
		int dataType = relateDataBaseTableField.getDataType();
		String field_name = relateDataBaseTableField.getField_name();
		Boolean isNullable = relateDataBaseTableField.getIs_nullable();
		String default_value = relateDataBaseTableField.getDefault_value();

		SchemaBuilder.FieldBuilder name = fields.name(field_name);
		SchemaBuilder.FieldTypeBuilder type = name.type();
		if (isNullable) {
			type.nullable();
		}
		switch (dataType) {
			case Types.BIT:
				try {
					type.booleanType().booleanDefault(Boolean.parseBoolean(default_value));
				} catch (Exception e) {
					type.booleanType().noDefault();
				}
				break;
			case Types.TINYINT:
			case Types.SMALLINT:
			case Types.INTEGER:
				try {
					type.intType().intDefault(Integer.parseInt(default_value));
				} catch (Exception e) {
					type.intType().noDefault();
				}
				break;
			case Types.BIGINT:
				try {
					type.longType().longDefault(Long.parseLong(default_value));
				} catch (Exception e) {
					type.longType().noDefault();
				}
				break;
			case Types.FLOAT:
			case Types.REAL:
				try {
					type.floatType().floatDefault(Float.parseFloat(default_value));
				} catch (Exception e) {
					type.floatType().noDefault();
				}
				break;
			case Types.NUMERIC:
			case Types.DOUBLE:
				try {
					type.doubleType().doubleDefault(Double.parseDouble(default_value));
				} catch (Exception e) {
					type.doubleType().noDefault();
				}
				break;
			case Types.VARBINARY:
			case Types.BINARY:
				type.bytesType();
				break;
			case Types.VARCHAR:
			case Types.CHAR:
			case Types.LONGNVARCHAR:
			case Types.CLOB:
			case Types.BLOB:
				if (default_value != null) {
					type.stringType().stringDefault(default_value);
				} else {
					type.stringType().noDefault();
				}
				break;
			case Types.DATE:
				switch (sourceDatabaseType) {
					case ORACLE:
					case DAMENG:
						try {
							name.type(TIMESTAMP_MILLIS_SCHEMA)
									.withDefault(TIMESTAMP_MILLIS_CONVERSION.toLong(Instant.parse(default_value), TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis()));
						} catch (Exception e) {
							name.type(TIMESTAMP_MILLIS_SCHEMA).noDefault();
						}

						break;

					case MYSQL:
					case MARIADB:
					case MYSQL_PXC:
					case KUNDB:
					case ADB_MYSQL:
					case ALIYUN_MYSQL:
					case ALIYUN_MARIADB:
					case MSSQL:
					case ALIYUN_MSSQL:
					case SYBASEASE:
						try {
							name.type(DATE_SCHEMA)
									.withDefault(DATE_CONVERSION.toInt(LocalDate.parse(default_value), DATE_SCHEMA, LogicalTypes.date()));
						} catch (Exception e) {
							name.type(DATE_SCHEMA).noDefault();
						}

						break;

					default:
						break;
				}
				break;

			case Types.TIMESTAMP:
				try {
					name.type(TIMESTAMP_MILLIS_SCHEMA)
							.withDefault(TIMESTAMP_MILLIS_CONVERSION.toLong(Instant.parse(default_value), TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis()));
				} catch (Exception e) {
					name.type(TIMESTAMP_MILLIS_SCHEMA).noDefault();
				}

				break;

			case Types.DECIMAL:
				Integer scale = relateDataBaseTableField.getScale();
				Integer precision = relateDataBaseTableField.getPrecision();
				Schema decimalSchema;
				if (scale != null && scale > 0) {
					decimalSchema = LogicalTypes.decimal(precision, scale).addToSchema(Schema.create(Schema.Type.BYTES));
				} else {
					decimalSchema = LogicalTypes.decimal(precision).addToSchema(Schema.create(Schema.Type.BYTES));
				}

				try {
					name.type(decimalSchema).withDefault(new BigDecimal(default_value));
				} catch (Exception e) {
					name.type(decimalSchema).noDefault();
				}
				break;

			default:
				break;
		}
	}

	private static Boolean isNullable(String is_nullable) {
		Boolean ret = false;
		if (is_nullable != null && is_nullable.equals(RelateDatabaseField.NULLABLE)) {
			ret = true;
		}
		return ret;
	}

	public static void main(String[] args) {
//        DateConversion conversion = new DateConversion();
//        TimestampMillisConversion timestampMillisConversion = new TimestampMillisConversion();
//
		Schema schema = SchemaBuilder.record("User").namespace("io.tapdata")
				.fields()
				.name("name").type().stringType().noDefault()
				.name("favorite_number").type().intType().noDefault()
				.name("favorite_color").type().nullable().stringType().noDefault()
				.name("ts").type(TIMESTAMP_MILLIS_SCHEMA).noDefault()
				.name("start_date").type(DATE_SCHEMA).noDefault()
				.name("oracle_date").type(TIMESTAMP_MILLIS_SCHEMA).noDefault()
				.name("aaaa").type().nullType().nullDefault()
				.endRecord();

		System.out.println(schema.toString(true));

//        GenericRecord user1 = new GenericData.Record(schema);
//        user1.put("name", "ply");
//        user1.put("favorite_number", 256);
//        user1.put("ts", System.currentTimeMillis());
//        user1.put("start_date", (int) conversion.toInt(LocalDate.parse("1970-01-03"), DATE_SCHEMA, LogicalTypes.date()));
//        user1.put("oracle_date", timestampMillisConversion.toLong(Instant.ofEpochMilli(new Date().getTime()), TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis()));
//
//        OutputStream outputStream = new ByteArrayOutputStream();
//        try {
//            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
//
//            /*JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(schema, outputStream, true);
//            datumWriter.write(user1, jsonEncoder);
//            jsonEncoder.flush();*/
//
//            /*BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(outputStream, null);
//            datumWriter.write(user1, binaryEncoder);
//            binaryEncoder.flush();
//            byte[] bytes = ((ByteArrayOutputStream) outputStream).toByteArray();
//            System.out.println(DatatypeConverter.printHexBinary(bytes));*/
//
//            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
//            dataFileWriter.create(schema, outputStream);
//            dataFileWriter.append(user1);
//
//            byte[] bytes = ((ByteArrayOutputStream) outputStream).toByteArray();
//            System.out.println(((ByteArrayOutputStream) outputStream).toString("UTF-8"));
//
//            dataFileWriter.close();
//            outputStream.close();
//
//            KafkaProducer<String, String> producer = KafkaUtil.createProducer(new Connections());
//            producer.send(new ProducerRecord<>("test", "a", ((ByteArrayOutputStream) outputStream).toString("UTF-8")));
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

		// Deserialize users from disk
//        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
//        DataFileReader<GenericRecord> dataFileReader = null;
//        SeekableByteArrayInput seekableByteArrayInput = new SeekableByteArrayInput(((ByteArrayOutputStream) outputStream).toByteArray());
//        try {
//            dataFileReader = new DataFileReader<GenericRecord>(seekableByteArrayInput, datumReader);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        GenericRecord user = null;
//        while (dataFileReader.hasNext()) {
//            try {
//                user = dataFileReader.next(user);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            System.out.println(user);
//        }

        /*List<RelateDataBaseTable> relateDataBaseTables = new ArrayList<>();
        RelateDataBaseTable t1 = new RelateDataBaseTable();
        List<RelateDatabaseField> fields = t1.getFields();
        fields = new ArrayList<>();
        RelateDatabaseField f1 = new RelateDatabaseField("name", "user", "VARCHAR");
        f1.setDefault_value("default");
        f1.setDataType(Types.VARCHAR);
        f1.setIs_nullable(RelateDatabaseField.NULLABLE);
        fields.add(f1);
        RelateDatabaseField f2 = new RelateDatabaseField("age", "user", "INTEGER");
        f2.setDataType(Types.INTEGER);
        fields.add(f2);
        RelateDatabaseField f3 = new RelateDatabaseField("ts", "user", "DATE");
        f3.setDataType(Types.DATE);
        fields.add(f3);
        t1.setFields(fields);
        t1.setTable_name("user");
        relateDataBaseTables.add(t1);

        Map<String, Schema> tapdata = getAvroSchemaMap(relateDataBaseTables, null, DatabaseTypeEnum.ORACLE, "TAPDATA");
        tapdata.forEach((k, v) -> {
            System.out.println(k + ":");
            System.out.println(v.toString(true));
        });*/

//        List<MessageEntity> messageEntities = new ArrayList<>();
//
//        for (int i = 0; i < 10; i++) {
//            MessageEntity msg = new MessageEntity();
//            messageEntities.add(msg);
//
//            Map<String, Object> data = new HashMap<>();
//            data.put("id", new Integer(i));
//            data.put("name", "sam");
//            data.put("f", 1.2f);
//            data.put("l", 2939l);
//            data.put("d", 2.34d);
//            data.put("date", new java.sql.Date(System.currentTimeMillis()));
////            data.put("time", new java.sql.Time(System.currentTimeMillis()));
////            data.put("timestamp", new Timestamp(System.currentTimeMillis()));
////            data.put("javaData", new Date());
//
//            msg.setAfter(data);
//        }
//
//        AvroUtil avroUtil = new AvroUtil();
//        Schema avroSchemaByTablename = avroUtil.getAvroSchemaByTablename(messageEntities.get(0).getAfter(), "", "user");
////        System.out.println(avroSchemaByTablename.toString(true));
//
//        byte[] bytes = AvroUtil.messages2AvroBytes(messageEntities, AvroUtil.FILE_WRITER, avroSchemaByTablename, LogManager.getLogger(AvroUtil.class));
//        System.out.println(Arrays.toString(bytes));
//
//        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(avroSchemaByTablename);
//        DataFileReader<GenericRecord> dataFileReader = null;
//        SeekableByteArrayInput seekableByteArrayInput = new SeekableByteArrayInput(bytes);
//        try {
//            dataFileReader = new DataFileReader<>(seekableByteArrayInput, datumReader);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        GenericRecord user = null;
//        while (dataFileReader.hasNext()) {
//            try {
//                user = dataFileReader.next(user);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            System.out.println(user);
//        }
	}
}

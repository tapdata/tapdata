package com.tapdata.constant;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.tapdata.entity.schema.type.TapType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by tapdata on 12/12/2017.
 */
public class JSONUtil {

  private static Logger logger = LogManager.getLogger(JSONUtil.class);

  protected static ObjectMapper mapper;

  static {
    mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    SimpleModule simpleModule = new SimpleModule();
    simpleModule.addDeserializer(TapType.class, new TapTypeDeserializer());
    mapper.registerModule(simpleModule);
    mapper.registerModule(new JavaTimeModule());
  }

  /**
   * Note that after using this function,
   * should call {@link JSONUtil#enableFeature(com.fasterxml.jackson.databind.SerializationFeature)} to restore the feature
   *
   * @param serializationFeature
   */
  public static void disableFeature(SerializationFeature serializationFeature) {
    mapper.disable(serializationFeature);
  }

  public static void enableFeature(SerializationFeature serializationFeature) {
    mapper.enable(serializationFeature);
  }

  public static <T> List<T> json2List(String json, Class<T> classz) throws IOException {
    List<T> list;
    try {
      TypeFactory typeFactory = mapper.getTypeFactory();
      list = mapper.readValue(json, typeFactory.constructCollectionType(List.class, classz));
//      list = InstanceFactory.instance(JsonParser.class).fromJson(json, new TypeHolder<List<T>>() {
//        },
//        TapConstants.abstractClassDetectors);
    } catch (Throwable e) {
      throw new IOException("parse json to " + classz.getName() + " list failed\n" + json, e);
    }
    return list;
  }

  public static String obj2Json(Object object) throws JsonProcessingException {
    String json;
    try {
      json = mapper.writeValueAsString(object);
    } catch (JsonProcessingException e) {
      logger.error("convert object to json failed.", e);
      logger.info(object);
      throw e;
    }
    return json;
  }

  public static String obj2JsonPretty(Object object) throws JsonProcessingException {
    String json;
    try {
      json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(object);
    } catch (JsonProcessingException e) {
      logger.error("convert object to json format fail", e);
      logger.info(object);
      throw e;
    }
    return json;
  }

  public static Map<String, Object> json2Map(String json) throws IOException {
    Map<String, Object> map;
    try {
      map = mapper.readValue(json, new TypeReference<HashMap<String, Object>>() {
      });
//      map = InstanceFactory.instance(JsonParser.class).fromJson(json, new TypeHolder<HashMap<String, Object>>() {
//        },
//        TapConstants.abstractClassDetectors);
    } catch (Throwable e) {
      throw new IOException("parse json to map failed\n" + json, e);
    }

    return map;
  }

  public static <T> T json2POJO(String json, Class<T> className) throws IOException {
    T pojo;
    try {
      pojo = mapper.readValue(json, className);
//      pojo = InstanceFactory.instance(JsonParser.class).fromJson(json, className,
//        TapConstants.abstractClassDetectors);
    } catch (Throwable e) {
      throw new IOException("parse json to " + className.getName() + " failed\n" + json, e);
    }

    return pojo;
  }

  public static <T> T json2POJO(String json, TypeReference<T> typeReference) throws IOException {
    T pojo;
    try {
      pojo = mapper.readValue(json, typeReference);
//      pojo = InstanceFactory.instance(JsonParser.class).fromJson(json, typeReference.getType(),
//        TapConstants.abstractClassDetectors);
    } catch (Throwable e) {
      throw new IOException("parse json to " + typeReference.getType().getTypeName() + " failed\n" + json, e);
    }

    return pojo;
  }

  public static <T> T map2POJO(Map map, Class<T> className) {

    return mapper.convertValue(map, className);
  }

  public static <T> T map2POJO(Map map, TypeReference<T> typeReference) {
    return mapper.convertValue(map, typeReference);
  }

  public static String map2Json(Map map) throws JsonProcessingException {
    return mapper.writeValueAsString(map);
  }

  public static String map2JsonPretty(Map map) throws JsonProcessingException {
    return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(map);
  }

  public static void main(String[] args) throws Exception {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    SimpleModule simpleModule = new SimpleModule();
    simpleModule.addDeserializer(TapType.class, new TapTypeDeserializer());
    objectMapper.registerModule(simpleModule);
    objectMapper.registerModule(new JavaTimeModule());
    /*TapString tapString = new TapString(10L, false);
    tapString.setDefaultValue(11L);
    tapString.setByteRatio(1);
    String json = objectMapper.writeValueAsString(tapString);
//    String json = "[{\"lastUpdate\":1652347748818,\"nameFieldMap\":{\"CLAIM_ID\":{\"dataType\":\"varchar(12)\",\"nullable\":false,\"name\":\"CLAIM_ID\",\"partitionKeyPos\":null,\"pos\":1,\"primaryKeyPos\":1,\"foreignKeyTable\":null,\"foreignKeyField\":null,\"defaultValue\":null,\"autoInc\":false,\"autoIncStartValue\":null,\"check\":null,\"comment\":null,\"constraint\":null,\"tapType\":{\"bytes\":12,\"fixed\":null,\"defaultValue\":1,\"byteRatio\":1,\"tapValueClass\":\"io.tapdata.entity.schema.value.TapStringValue\"},\"primaryKey\":true,\"partitionKey\":false},\"POLICY_ID\":{\"dataType\":\"varchar(12)\",\"nullable\":true,\"name\":\"POLICY_ID\",\"partitionKeyPos\":null,\"pos\":2,\"primaryKeyPos\":null,\"foreignKeyTable\":null,\"foreignKeyField\":null,\"defaultValue\":null,\"autoInc\":false,\"autoIncStartValue\":null,\"check\":null,\"comment\":null,\"constraint\":null,\"tapType\":{\"bytes\":12,\"fixed\":null,\"defaultValue\":1,\"byteRatio\":1,\"tapValueClass\":\"io.tapdata.entity.schema.value.TapStringValue\"},\"primaryKey\":false,\"partitionKey\":false},\"CLAIM_DATE\":{\"dataType\":\"datetime\",\"nullable\":true,\"name\":\"CLAIM_DATE\",\"partitionKeyPos\":null,\"pos\":3,\"primaryKeyPos\":null,\"foreignKeyTable\":null,\"foreignKeyField\":null,\"defaultValue\":null,\"autoInc\":false,\"autoIncStartValue\":null,\"check\":null,\"comment\":null,\"constraint\":null,\"tapType\":{\"withTimeZone\":null,\"bytes\":null,\"min\":-3.0610224E10,\"max\":2.534023008E11,\"fraction\":0,\"defaultFraction\":0,\"tapValueClass\":\"io.tapdata.entity.schema.value.TapDateTimeValue\"},\"primaryKey\":false,\"partitionKey\":false},\"SETTLED_DATE\":{\"dataType\":\"datetime\",\"nullable\":true,\"name\":\"SETTLED_DATE\",\"partitionKeyPos\":null,\"pos\":4,\"primaryKeyPos\":null,\"foreignKeyTable\":null,\"foreignKeyField\":null,\"defaultValue\":null,\"autoInc\":false,\"autoIncStartValue\":null,\"check\":null,\"comment\":null,\"constraint\":null,\"tapType\":{\"withTimeZone\":null,\"bytes\":null,\"min\":-3.0610224E10,\"max\":2.534023008E11,\"fraction\":0,\"defaultFraction\":0,\"tapValueClass\":\"io.tapdata.entity.schema.value.TapDateTimeValue\"},\"primaryKey\":false,\"partitionKey\":false},\"CLAIM_AMOUNT\":{\"dataType\":\"decimal(30,2)\",\"nullable\":true,\"name\":\"CLAIM_AMOUNT\",\"partitionKeyPos\":null,\"pos\":5,\"primaryKeyPos\":null,\"foreignKeyTable\":null,\"foreignKeyField\":null,\"defaultValue\":null,\"autoInc\":false,\"autoIncStartValue\":null,\"check\":null,\"comment\":null,\"constraint\":null,\"tapType\":{\"bit\":null,\"fixed\":null,\"unsigned\":null,\"zerofill\":null,\"minValue\":-999999999999999999999999999999,\"maxValue\":999999999999999999999999999999,\"precision\":30,\"scale\":2,\"tapValueClass\":\"io.tapdata.entity.schema.value.TapNumberValue\"},\"primaryKey\":false,\"partitionKey\":false},\"SETTLED_AMOUNT\":{\"dataType\":\"decimal(30,2)\",\"nullable\":true,\"name\":\"SETTLED_AMOUNT\",\"partitionKeyPos\":null,\"pos\":6,\"primaryKeyPos\":null,\"foreignKeyTable\":null,\"foreignKeyField\":null,\"defaultValue\":null,\"autoInc\":false,\"autoIncStartValue\":null,\"check\":null,\"comment\":null,\"constraint\":null,\"tapType\":{\"bit\":null,\"fixed\":null,\"unsigned\":null,\"zerofill\":null,\"minValue\":-999999999999999999999999999999,\"maxValue\":999999999999999999999999999999,\"precision\":30,\"scale\":2,\"tapValueClass\":\"io.tapdata.entity.schema.value.TapNumberValue\"},\"primaryKey\":false,\"partitionKey\":false},\"CLAIM_REASON\":{\"dataType\":\"varchar(30)\",\"nullable\":true,\"name\":\"CLAIM_REASON\",\"partitionKeyPos\":null,\"pos\":7,\"primaryKeyPos\":null,\"foreignKeyTable\":null,\"foreignKeyField\":null,\"defaultValue\":null,\"autoInc\":false,\"autoIncStartValue\":null,\"check\":null,\"comment\":null,\"constraint\":null,\"tapType\":{\"bytes\":30,\"fixed\":null,\"defaultValue\":1,\"byteRatio\":1,\"tapValueClass\":\"io.tapdata.entity.schema.value.TapStringValue\"},\"primaryKey\":false,\"partitionKey\":false},\"LAST_CHANGE\":{\"dataType\":\"datetime(6)\",\"nullable\":true,\"name\":\"LAST_CHANGE\",\"partitionKeyPos\":null,\"pos\":8,\"primaryKeyPos\":null,\"foreignKeyTable\":null,\"foreignKeyField\":null,\"defaultValue\":null,\"autoInc\":false,\"autoIncStartValue\":null,\"check\":null,\"comment\":null,\"constraint\":null,\"tapType\":{\"withTimeZone\":null,\"bytes\":null,\"min\":-3.0610224E10,\"max\":2.534023008E11,\"fraction\":6,\"defaultFraction\":0,\"tapValueClass\":\"io.tapdata.entity.schema.value.TapDateTimeValue\"},\"primaryKey\":false,\"partitionKey\":false}},\"defaultPrimaryKeys\":null,\"indexList\":null,\"id\":\"CAR_CLAIM\",\"name\":\"CAR_CLAIM\",\"storageEngine\":null,\"charset\":null,\"comment\":null,\"pdkId\":null,\"pdkGroup\":null,\"pdkVersion\":null}]";
    System.out.println(json);
    TapType tapType = objectMapper.readValue(json, new TypeReference<TapType>() {
    });
    List<TapTable> tapTables = objectMapper.readValue("[{\"lastUpdate\":1652347748818,\"nameFieldMap\":{\"CLAIM_ID\":{\"dataType\":\"varchar(12)\",\"nullable\":false,\"name\":\"CLAIM_ID\",\"partitionKeyPos\":null,\"pos\":1,\"primaryKeyPos\":1,\"foreignKeyTable\":null,\"foreignKeyField\":null,\"defaultValue\":null,\"autoInc\":false,\"autoIncStartValue\":null,\"check\":null,\"comment\":null,\"constraint\":null,\"tapType\":{\"bytes\":12,\"fixed\":null,\"defaultValue\":1,\"byteRatio\":1,\"tapValueClass\":\"io.tapdata.entity.schema.value.TapStringValue\"},\"primaryKey\":true,\"partitionKey\":false},\"POLICY_ID\":{\"dataType\":\"varchar(12)\",\"nullable\":true,\"name\":\"POLICY_ID\",\"partitionKeyPos\":null,\"pos\":2,\"primaryKeyPos\":null,\"foreignKeyTable\":null,\"foreignKeyField\":null,\"defaultValue\":null,\"autoInc\":false,\"autoIncStartValue\":null,\"check\":null,\"comment\":null,\"constraint\":null,\"tapType\":{\"bytes\":12,\"fixed\":null,\"defaultValue\":1,\"byteRatio\":1,\"tapValueClass\":\"io.tapdata.entity.schema.value.TapStringValue\"},\"primaryKey\":false,\"partitionKey\":false},\"CLAIM_DATE\":{\"dataType\":\"datetime\",\"nullable\":true,\"name\":\"CLAIM_DATE\",\"partitionKeyPos\":null,\"pos\":3,\"primaryKeyPos\":null,\"foreignKeyTable\":null,\"foreignKeyField\":null,\"defaultValue\":null,\"autoInc\":false,\"autoIncStartValue\":null,\"check\":null,\"comment\":null,\"constraint\":null,\"tapType\":{\"withTimeZone\":null,\"bytes\":null,\"min\":-3.0610224E10,\"max\":2.534023008E11,\"fraction\":0,\"defaultFraction\":0,\"tapValueClass\":\"io.tapdata.entity.schema.value.TapDateTimeValue\"},\"primaryKey\":false,\"partitionKey\":false},\"SETTLED_DATE\":{\"dataType\":\"datetime\",\"nullable\":true,\"name\":\"SETTLED_DATE\",\"partitionKeyPos\":null,\"pos\":4,\"primaryKeyPos\":null,\"foreignKeyTable\":null,\"foreignKeyField\":null,\"defaultValue\":null,\"autoInc\":false,\"autoIncStartValue\":null,\"check\":null,\"comment\":null,\"constraint\":null,\"tapType\":{\"withTimeZone\":null,\"bytes\":null,\"min\":-3.0610224E10,\"max\":2.534023008E11,\"fraction\":0,\"defaultFraction\":0,\"tapValueClass\":\"io.tapdata.entity.schema.value.TapDateTimeValue\"},\"primaryKey\":false,\"partitionKey\":false},\"CLAIM_AMOUNT\":{\"dataType\":\"decimal(30,2)\",\"nullable\":true,\"name\":\"CLAIM_AMOUNT\",\"partitionKeyPos\":null,\"pos\":5,\"primaryKeyPos\":null,\"foreignKeyTable\":null,\"foreignKeyField\":null,\"defaultValue\":null,\"autoInc\":false,\"autoIncStartValue\":null,\"check\":null,\"comment\":null,\"constraint\":null,\"tapType\":{\"bit\":null,\"fixed\":null,\"unsigned\":null,\"zerofill\":null,\"minValue\":-999999999999999999999999999999,\"maxValue\":999999999999999999999999999999,\"precision\":30,\"scale\":2,\"tapValueClass\":\"io.tapdata.entity.schema.value.TapNumberValue\"},\"primaryKey\":false,\"partitionKey\":false},\"SETTLED_AMOUNT\":{\"dataType\":\"decimal(30,2)\",\"nullable\":true,\"name\":\"SETTLED_AMOUNT\",\"partitionKeyPos\":null,\"pos\":6,\"primaryKeyPos\":null,\"foreignKeyTable\":null,\"foreignKeyField\":null,\"defaultValue\":null,\"autoInc\":false,\"autoIncStartValue\":null,\"check\":null,\"comment\":null,\"constraint\":null,\"tapType\":{\"bit\":null,\"fixed\":null,\"unsigned\":null,\"zerofill\":null,\"minValue\":-999999999999999999999999999999,\"maxValue\":999999999999999999999999999999,\"precision\":30,\"scale\":2,\"tapValueClass\":\"io.tapdata.entity.schema.value.TapNumberValue\"},\"primaryKey\":false,\"partitionKey\":false},\"CLAIM_REASON\":{\"dataType\":\"varchar(30)\",\"nullable\":true,\"name\":\"CLAIM_REASON\",\"partitionKeyPos\":null,\"pos\":7,\"primaryKeyPos\":null,\"foreignKeyTable\":null,\"foreignKeyField\":null,\"defaultValue\":null,\"autoInc\":false,\"autoIncStartValue\":null,\"check\":null,\"comment\":null,\"constraint\":null,\"tapType\":{\"bytes\":30,\"fixed\":null,\"defaultValue\":1,\"byteRatio\":1,\"tapValueClass\":\"io.tapdata.entity.schema.value.TapStringValue\"},\"primaryKey\":false,\"partitionKey\":false},\"LAST_CHANGE\":{\"dataType\":\"datetime(6)\",\"nullable\":true,\"name\":\"LAST_CHANGE\",\"partitionKeyPos\":null,\"pos\":8,\"primaryKeyPos\":null,\"foreignKeyTable\":null,\"foreignKeyField\":null,\"defaultValue\":null,\"autoInc\":false,\"autoIncStartValue\":null,\"check\":null,\"comment\":null,\"constraint\":null,\"tapType\":{\"withTimeZone\":null,\"bytes\":null,\"min\":-3.0610224E10,\"max\":2.534023008E11,\"fraction\":6,\"defaultFraction\":0,\"tapValueClass\":\"io.tapdata.entity.schema.value.TapDateTimeValue\"},\"primaryKey\":false,\"partitionKey\":false}},\"defaultPrimaryKeys\":null,\"indexList\":null,\"id\":\"CAR_CLAIM\",\"name\":\"CAR_CLAIM\",\"storageEngine\":null,\"charset\":null,\"comment\":null,\"pdkId\":null,\"pdkGroup\":null,\"pdkVersion\":null}]",
      new TypeReference<List<TapTable>>() {
      });
    System.out.println(tapType);*/
    Map<String, Object> map = objectMapper.readValue("{\n" +
      "  \"timestamp\": \"2022-05-13T11:04:21Z\",\n" +
      "  \"version\": \"@env.VERSION@\",\n" +
      "  \"gitCommitId\": \"@env.DAAS_GIT_VERSION@\"\n" +
      "}", new TypeReference<HashMap<String, Object>>() {
    });
    System.out.println(map);
  }

  static class TapTypeDeserializer extends JsonDeserializer<TapType> {
    @Override
    public TapType deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
      ObjectCodec codec = p.getCodec();
      TreeNode treeNode = codec.readTree(p);
      TreeNode type = treeNode.get("type");
      int typeInt = (int) ((IntNode) type).numberValue();
      Class<? extends TapType> tapTypeClass = TapType.getTapTypeClass((byte) typeInt);
      if (null != tapTypeClass) {
        return codec.treeToValue(treeNode, tapTypeClass);
      } else {
        throw new RuntimeException("Unsupported tap type: " + typeInt);
      }
    }
  }
}

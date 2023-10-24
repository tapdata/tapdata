package io.tapdata.entity.conversion;

import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.conversion.TableFieldTypesGenerator;
import io.tapdata.entity.conversion.TargetTypesGenerator;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.value.TapYearValue;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.error.CoreException;
import io.tapdata.pdk.core.error.PDKRunnerErrorCodes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static org.junit.jupiter.api.Assertions.*;

class TargetTypesGeneratorTest {
    private TargetTypesGenerator targetTypesGenerator;
    private TableFieldTypesGenerator tableFieldTypesGenerator;
    private TapCodecsFilterManager targetCodecFilterManager;
    private TapCodecsRegistry codecRegistry;

    @BeforeEach
    void beforeEach() {
        targetTypesGenerator = InstanceFactory.instance(TargetTypesGenerator.class);
        if(targetTypesGenerator == null)
            throw new CoreException(PDKRunnerErrorCodes.SOURCE_TARGET_TYPES_GENERATOR_NOT_FOUND, "TargetTypesGenerator's implementation is not found in current classloader");
        tableFieldTypesGenerator = InstanceFactory.instance(TableFieldTypesGenerator.class);
        if(tableFieldTypesGenerator == null)
            throw new CoreException(PDKRunnerErrorCodes.SOURCE_TABLE_FIELD_TYPES_GENERATOR_NOT_FOUND, "TableFieldTypesGenerator's implementation is not found in current classloader");
        codecRegistry = TapCodecsRegistry.create();
        targetCodecFilterManager = TapCodecsFilterManager.create(codecRegistry);
    }
    /*
    @Test
    void convert() {
        String sourceTypeExpression = "{\n" +
                "    \"tinyint[($bit)][unsigned][zerofill]\": {\"bit\": 1, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"smallint[($bit)][unsigned][zerofill]\": {\"bit\": 4, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"mediumint[($bit)][unsigned][zerofill]\": {\"bit\": 8, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"int[($bit)][unsigned][zerofill]\": {\"bit\": 32, \"unsigned\": \"unsigned\", \"zerofill\": \"zerofill\", \"to\": \"TapNumber\"},\n" +
                "    \"bigint($bit)[unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"bigint[unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"float[($bit)][unsigned][zerofill]\": {\"bit\": 16, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"double[($bit)][unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"decimal($precision,$scale)[theUnsigned][theZerofill]\": {\"precision\":[1, 65], \"scale\": [-3, 30], \"unsigned\": \"theUnsigned\", \"zerofill\": \"theZerofill\", \"precisionDefault\": 10, \"scaleDefault\": 0, \"to\": \"TapNumber\"},\n" +
                "    \"date\": {\"range\": [\"1000-01-01\", \"9999-12-31\"], \"gmt\": 8, \"to\": \"TapDate\"},\n" +
                "    \"time\": {\"range\": [\"-838:59:59\",\"838:59:59\"], \"gmt\": 8, \"to\": \"TapTime\"},\n" +
                "    \"year\": {\"range\": [1901, 2155], \"to\": \"TapYear\"},\n" +
                "    \"datetime\": {\"range\": [\"1000-01-01 00:00:00\", \"9999-12-31 23:59:59\"], \"gmt\": 8, \"to\": \"TapDateTime\"},\n" +
                "    \"timestamp\": {\"to\": \"TapDateTime\"},\n" +
                "    \"char[($byte)]\": {\"byte\": 255, \"to\": \"TapString\"},\n" +
                "    \"varchar[($byte)]\": {\"byte\": \"64k\", \"byteRatio\": 3, \"fixed\": false, \"to\": \"TapString\"},\n" +
                "    \"tinyblob\": {\"byte\": 255, \"to\": \"TapBinary\"},\n" +
                "    \"tinytext\": {\"byte\": 255, \"to\": \"TapString\"},\n" +
                "    \"blob\": {\"byte\": \"64k\", \"to\": \"TapBinary\"},\n" +
                "    \"text\": {\"byte\": \"64k\", \"to\": \"TapString\"},\n" +
                "    \"mediumblob\": {\"byte\": \"16m\", \"to\": \"TapBinary\"},\n" +
                "    \"mediumtext\": {\"byte\": \"16m\", \"to\": \"TapString\"},\n" +
                "    \"longblob\": {\"byte\": \"4g\", \"to\": \"TapBinary\"},\n" +
                "    \"longtext\": {\"byte\": \"4g\", \"to\": \"TapString\"},\n" +
                "    \"bit($byte)\": {\"byte\": 8, \"to\": \"TapBinary\"},\n" +
                "    \"binary($byte)\": {\"byte\": 255, \"to\": \"TapBinary\"},\n" +
                "    \"varbinary($byte)\": {\"byte\": 255, \"fixed\": false, \"to\": \"TapBinary\"},\n" +
                "    \"[varbinary]($byte)[ABC$hi]aaaa[DDD[AAA|BBB]]\": {\"byte\": 33333, \"fixed\": false, \"to\": \"TapBinary\"}\n" +
                "}";
        String targetTypeExpression = "{\n" +
//                "    \"boolean\":{\"bit\":8, \"unsigned\":\"\", \"to\":\"TapNumber\"},\n" +
//                "    \"tinyint\":{\"bit\":8, \"to\":\"TapNumber\"},\n" +
//                "    \"smallint\":{\"bit\":16, \"to\":\"TapNumber\"},\n" +
//                "    \"int\":{\"bit\":32, \"to\":\"TapNumber\"},\n" +
//                "    \"bigint\":{\"bit\":64, \"to\":\"TapNumber\"},\n" +
//                "    \"largeint\":{\"bit\":128, \"to\":\"TapNumber\"},\n" +
//                "    \"float\":{\"bit\":32, \"to\":\"TapNumber\"},\n" +
//                "    \"myint[($bit)][unsigned]\":{\"bit\":32, \"unsigned\":\"unsigned\", \"to\":\"TapNumber\"},\n" +
//                "    \"double\":{\"bit\":64, \"to\":\"TapNumber\"},\n" +
//                "    \"decimal[($precision,$scale)]\":{\"precision\": [1, 27], \"defaultPrecision\": 10, \"scale\": [0, 9], \"defaultScale\": 0, \"to\": \"TapNumber\"},\n" +
//                "    \"date\":{\"byte\":3, \"range\":[\"0000-01-01\", \"9999-12-31\"], \"to\":\"TapDate\"},\n" +
//                "    \"datetime\":{\"byte\":8, \"range\":[\"0000-01-01 00:00:00\",\"9999-12-31 23:59:59\"],\"to\":\"TapDateTime\"},\n" +
//                "    \"char[($byte)]\":{\"byte\":255, \"byteRatio\": 2, \"to\": \"TapString\", \"defaultByte\": 1},\n" +
                "    \"varchar[($byte)]\":{\"byte\":\"65535\", \"to\":\"TapString\"},\n" +
                "    \"string\":{\"byte\":\"2147483643\", \"to\":\"TapString\"},\n" +
//                "    \"HLL\":{\"byte\":\"16385\", \"to\":\"TapNumber\", \"queryOnly\":true}\n" +
                "}";

        TapTable sourceTable = table("test");
        sourceTable
//                .add(field("tinytext", "tinytext"))
//                .add(field("datetime", "datetime"))
//                .add(field("bigint", "bigint"))
//                .add(field("bigint unsigned", "bigint unsigned"))
//                .add(field("bigint(32) unsigned", "bigint(32) unsigned"))
//                .add(field("char(300)", "char(300)"))
//                .add(field("decimal(27, -3)", "decimal(27, -3)"))
//                .add(field("longtext", "longtext")) // exceed the max of target types
//                .add(field("double(32)", "double(32)"))
//                .add(field("mediumtext", "mediumtext"))
                .add(field("bit(8)", "bit(8)")) //no binary in target types
//                .add(field("binary(200)", "binary(200)"))
//                .add(field("varchar(10)", "varchar(10)"))
//                .add(field("timestamp", "timestamp"))
//                .add(field("mediumint unsigned", "mediumint unsigned"))

        ;
        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
//        assertNotNull(sourceTable.getNameFieldMap().get("tinytext").getTapType());
//        assertNotNull(sourceTable.getNameFieldMap().get("char(300)").getTapType());
//        assertNotNull(sourceTable.getNameFieldMap().get("bit(8)").getTapType());

        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);
        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();

    }*/

    @Test
    void convertTest() {
        String sourceTypeExpression = "{\n" +
                "    \"int[($bit)][unsigned][zerofill]\": {\"bit\": 32, \"bitRatio\": 3, \"unsigned\": \"unsigned\", \"zerofill\": \"zerofill\", \"to\": \"TapNumber\"},\n" +
                "    \"varchar[($byte)]\": {\"byte\": \"64k\", \"byteRatio\": 3, \"fixed\": false, \"to\": \"TapString\"},\n" +
                "    \"decimal($precision,$scale)[theUnsigned][theZerofill]\": {\"precision\":[1, 65], \"scale\": [-3, 30], \"unsigned\": \"theUnsigned\", \"zerofill\": \"theZerofill\", \"precisionDefault\": 10, \"scaleDefault\": 0, \"to\": \"TapNumber\"},\n" +
                "    \"longtext\": {\"byte\": \"4g\", \"to\": \"TapString\"},\n" +

                "    \"tinyint[($bit)][unsigned][zerofill]\": {\"bit\": 1, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"smallint[($bit)][unsigned][zerofill]\": {\"bit\": 4, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"mediumint[($bit)][unsigned][zerofill]\": {\"bit\": 8, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"bigint($bit)[unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"bigint[unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"float[($bit)][unsigned][zerofill]\": {\"bit\": 16, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"double[($bit)][unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"date\": {\"range\": [\"1000-01-01\", \"9999-12-31\"], \"gmt\": 8, \"to\": \"TapDate\"},\n" +
                "    \"time\": {\"range\": [\"-838:59:59\",\"838:59:59\"], \"gmt\": 8, \"to\": \"TapTime\"},\n" +
                "    \"year\": {\"range\": [1901, 2155], \"to\": \"TapYear\"},\n" +
                "    \"datetime\": {\"range\": [\"1000-01-01 00:00:00\", \"9999-12-31 23:59:59\"], \"pattern\": \"yyyy-MM-dd HH:mm:ss\", \"to\": \"TapDateTime\"},\n" +
                "    \"timestamp\": {\"to\": \"TapDateTime\"},\n" +
                "    \"char[($byte)]\": {\"byte\": 255, \"to\": \"TapString\"},\n" +
                "    \"tinyblob\": {\"byte\": 255, \"to\": \"TapBinary\"},\n" +
                "    \"tinytext\": {\"byte\": 255, \"to\": \"TapString\"},\n" +
                "    \"blob\": {\"byte\": \"64k\", \"to\": \"TapBinary\"},\n" +
                "    \"text\": {\"byte\": \"64k\", \"to\": \"TapString\"},\n" +
                "    \"mediumblob\": {\"byte\": \"16m\", \"to\": \"TapBinary\"},\n" +
                "    \"mediumtext\": {\"byte\": \"16m\", \"to\": \"TapString\"},\n" +
                "    \"longblob\": {\"byte\": \"4g\", \"to\": \"TapBinary\"},\n" +
                "    \"bit($byte)\": {\"byte\": 8, \"to\": \"TapBinary\"},\n" +
                "    \"binary($byte)\": {\"byte\": 255, \"to\": \"TapBinary\"},\n" +
                "    \"varbinary($byte)\": {\"byte\": 255, \"fixed\": false, \"to\": \"TapBinary\"},\n" +
                "    \"[varbinary]($byte)[ABC$hi]aaaa[DDD[AAA|BBB]]\": {\"byte\": 33333, \"fixed\": false, \"to\": \"TapBinary\"}\n" +
                "}";
        String targetTypeExpression = "{\n" +
                "    \"char[($byte)]\":{\"byte\":255, \"byteRatio\": 2, \"to\": \"TapString\", \"defaultByte\": 1},\n" +
                "    \"decimal[($precision,$scale)]\":{\"precision\": [1, 27], \"defaultPrecision\": 10, \"scale\": [0, 9], \"defaultScale\": 0, \"to\": \"TapNumber\"},\n" +
                "    \"string\":{\"byte\":\"2147483643\", \"to\":\"TapString\"},\n" +
                "    \"myint[($bit)][unsigned]\":{\"bit\":48, \"bitRatio\": 2, \"unsigned\":\"unsigned\", \"to\":\"TapNumber\"},\n" +

                "    \"largeint\":{\"bit\":128, \"to\":\"TapNumber\"},\n" +
                "    \"boolean\":{\"bit\":8, \"unsigned\":\"\", \"to\":\"TapNumber\"},\n" +
                "    \"tinyint\":{\"bit\":8, \"to\":\"TapNumber\"},\n" +
                "    \"smallint\":{\"bit\":16, \"to\":\"TapNumber\"},\n" +
                "    \"int\":{\"bit\":32, \"to\":\"TapNumber\"},\n" +
                "    \"bigint\":{\"bit\":64, \"to\":\"TapNumber\"},\n" +
                "    \"float\":{\"bit\":32, \"to\":\"TapNumber\"},\n" +
                "    \"double\":{\"bit\":64, \"to\":\"TapNumber\"},\n" +
                "    \"date\":{\"byte\":3, \"range\":[\"1000-01-01\", \"9999-12-31\"], \"to\":\"TapDate\"},\n" +
                "    \"datetime\":{\"byte\":8, \"range\":[\"1000-01-01 00:00:00\",\"9999-12-31 23:59:59\"],\"pattern\": \"yyyy-MM-dd HH:mm:ss\", \"to\":\"TapDateTime\"},\n" +
                "    \"varchar[($byte)]\":{\"byte\":\"65535\", \"to\":\"TapString\"},\n" +
                "    \"HLL\":{\"byte\":\"16385\", \"to\":\"TapNumber\", \"queryOnly\":true}\n" +
                "}";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("int(32) unsigned", "int(32) unsigned"))
                .add(field("longtext", "longtext")) // exceed the max of target types
                .add(field("varchar(10)", "varchar(10)"))
                .add(field("decimal(20, -3)", "decimal(20, -3)"))


        ;
        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
//        assertNotNull(sourceTable.getNameFieldMap().get("tinytext").getTapType());
//        assertNotNull(sourceTable.getNameFieldMap().get("char(300)").getTapType());
//        assertNotNull(sourceTable.getNameFieldMap().get("bit(8)").getTapType());

        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();

        //源端一个bit等于3个bit， 目标端一个bit等于2个bit的case， 适用于解决bit有时是bit， 有时是byte的问题。
        //Source: "    \"int[($bit)][unsigned][zerofill]\": {\"bit\": 32, \"bitRatio\": 3, \"unsigned\": \"unsigned\", \"zerofill\": \"zerofill\", \"to\": \"TapNumber\"},\n" +
        //Target: "    \"myint[($bit)][unsigned]\":{\"bit\":48, \"bitRatio\": 2, \"unsigned\":\"unsigned\", \"to\":\"TapNumber\"},\n" +
        TapField int32unsignedField = nameFieldMap.get("int(32) unsigned");
        assertEquals("myint(48) unsigned", int32unsignedField.getDataType());
        assertEquals(96, ((TapNumber)int32unsignedField.getTapType()).getBit());

        //源端一个byte等于3个byte， 目标端一个byte等于2个byte的case， 适用于解决byte有时是byte， 有时是char的问题
        //Source: "    \"varchar[($byte)]\": {\"byte\": \"64k\", \"byteRatio\": 3, \"fixed\": false, \"to\": \"TapString\"},\n" +
        //Target: "    \"char[($byte)]\":{\"byte\":255, \"byteRatio\": 2, \"to\": \"TapString\", \"defaultByte\": 1},\n" +
        TapField varchar10Field = nameFieldMap.get("varchar(10)");
        assertEquals("char(10)", varchar10Field.getDataType());
        assertEquals(10, ((TapString)varchar10Field.getTapType()).getBytes());

        //源端scale是负数， 目标端不支持负数的case
        //Source: "    \"decimal($precision,$scale)[theUnsigned][theZerofill]\": {\"precision\":[1, 65], \"scale\": [-3, 30], \"unsigned\": \"theUnsigned\", \"zerofill\": \"theZerofill\", \"precisionDefault\": 10, \"scaleDefault\": 0, \"to\": \"TapNumber\"},\n" +
        //Target: "    \"decimal[($precision,$scale)]\":{\"precision\": [1, 27], \"defaultPrecision\": 10, \"scale\": [0, 9], \"defaultScale\": 0, \"to\": \"TapNumber\"},\n" +
        TapField decimal273 = nameFieldMap.get("decimal(20, -3)");
        assertEquals("decimal(23,0)", decimal273.getDataType());
        assertEquals(-3, ((TapNumber)decimal273.getTapType()).getScale());

        //源端的类型大于任何目标端的类型， 因此在目标端选择尽可能大的类型
        //Source: "    \"longtext\": {\"byte\": \"4g\", \"to\": \"TapString\"},\n" +
        //Target: "    \"string\":{\"byte\":\"2147483643\", \"to\":\"TapString\"},\n" +
        TapField longtext = nameFieldMap.get("longtext");
        assertEquals("string", longtext.getDataType());
        assertEquals(4294967295L, ((TapString)longtext.getTapType()).getBytes());
    }

    @Test
    void convertPriorityTest() {
        String sourceTypeExpression = "{\n" +
                "    \"int[($bit)][unsigned][zerofill]\": {\"bit\": 32, \"unsigned\": \"unsigned\", \"zerofill\": \"zerofill\", \"to\": \"TapNumber\"},\n" +
                "}";
        String targetTypeExpression = "{\n" +
                "    \"myint[($bit)][unsigned]\":{\"bit\":32, \"unsigned\":\"unsigned\", \"to\":\"TapNumber\"},\n" +
                "    \"tinyint\":{\"bit\":32, \"priority\":1, \"to\":\"TapNumber\"},\n" +
                "    \"smallint\":{\"bit\":32, \"priority\":3, \"to\":\"TapNumber\"},\n" +
                "    \"int\":{\"bit\":32, \"priority\":2, \"to\":\"TapNumber\"},\n" +
                "}";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("int(32)", "int(32)"))

        ;
        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));

        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);
        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();

        TapField int32unsignedField = nameFieldMap.get("int(32)");
        assertEquals("tinyint", int32unsignedField.getDataType());
    }


    @Test
    void numberTest() {
        String sourceTypeExpression = "{\n" +
                "    \"int[($bit)][unsigned]\": {\"bit\": 32, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"decimal($precision,$scale)[unsigned]\": {\"precision\":[1, 65], \"scale\": [-3, 30], \"unsigned\": \"unsigned\", \"precisionDefault\": 10, \"scaleDefault\": 0, \"fixed\": true, \"to\": \"TapNumber\"},\n" +

                "    \"tinyint[($bit)][unsigned][zerofill]\": {\"bit\": 1, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"smallint[($bit)][unsigned][zerofill]\": {\"bit\": 4, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"mediumint[($bit)][unsigned][zerofill]\": {\"bit\": 8, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"bigint($bit)[unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"bigint[unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"float[($bit)][unsigned][zerofill]\": {\"bit\": 16, \"unsigned\": \"unsigned\", \"scale\": [ 0, 6], \"fixed\": false, \"to\": \"TapNumber\"},\n" +
                "    \"double[($bit)][unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"scale\": [ 0, 6], \"fixed\": false, \"to\": \"TapNumber\"},\n" +
                "}";
        String targetTypeExpression = "{" +
                "\"tinyint[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 8,\"precision\": 3,\"value\": [ -128, 127],\"unsignedValue\": [ 0, 255],\"unsigned\": \"unsigned\"},\n" +
                "\"smallint[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 16,\"value\": [ -32768, 32767],\"unsignedValue\": [ 0, 65535],\"unsigned\": \"unsigned\",\"precision\": 5},\n" +
                "\"mediumint[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 24,\"precision\": 7,\"value\": [ -8388608, 8388607],\"unsignedValue\": [ 0, 16777215],\"unsigned\": \"unsigned\"},\n" +
                "\"int[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 32,\"precision\": 10,\"value\": [ -2147483648, 2147483647],\"unsignedValue\": [ 0, 4294967295],\"unsigned\": \"unsigned\"},\n" +
                "\"bigint[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 64,\"precision\": 19,\"value\": [ -9223372036854775808, 9223372036854775807], \"unsignedValue\": [ 0, 18446744073709551615],\"unsigned\": \"unsigned\"},\n" +
                "\"superbigint\": {\"to\": \"TapNumber\",\"bit\": 640},\n" +
                "\"decimal[($precision,$scale)][unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 65],\"scale\": [ 0, 30],\"defaultPrecision\": 10,\"defaultScale\": 0,\"unsigned\": \"unsigned\", \"fixed\": true},\n" +
                "\"float($precision,$scale)[unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 30],\"defaultPrecision\": 10,\"scale\": [ 0, 30],\"value\": [ \"-3.402823466E+38\", \"3.402823466E+38\"],\"unsigned\": \"unsigned\",\"fixed\": false},\n" +
                "\"float\": {\"to\": \"TapNumber\",\"precision\": [ 1, 6],\"scale\": [ 0, 6],\"fixed\": false},\n" +
                "\"double\": {\"to\": \"TapNumber\",\"precision\": [ 1, 11],\"scale\": [ 0, 11],\"fixed\": true},\n" +
                "\"double[($precision,$scale)][unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 255],\"defaultPrecision\": 10,\"scale\": [ 0, 30],\"value\": [ \"-1.7976931348623157E+308\", \"1.7976931348623157E+308\"],\"unsigned\": \"unsigned\",\"fixed\": true}" +
                "}";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("int unsigned", "int unsigned"))
                .add(field("int(32)", "int(32)"))
                .add(field("decimal(65,30) unsigned", "decimal(65,30) unsigned"))
                .add(field("decimal(65,-3)", "decimal(65,-3)"))
                .add(field("decimal(55,-3)", "decimal(55,-3)"))
                .add(field("decimal(65,30)", "decimal(65,30)"))
                .add(field("float", "float"))
                .add(field("float unsigned", "float unsigned"))
                .add(field("float(8)", "float(8)"))
                .add(field("double(256) unsigned", "double(256) unsigned"))
                .add(field("double", "double"))
                .add(field("bigint(150)", "bigint(150)"))
                .add(field("bigint(50)", "bigint(50)"))
        ;
        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));

        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);
        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();

        TapField intUnsignedField = nameFieldMap.get("int unsigned");
        assertEquals("int unsigned", intUnsignedField.getDataType());

        TapField int32unsignedField = nameFieldMap.get("int(32)");
        assertEquals("int", int32unsignedField.getDataType());

        TapField decimal650Field = nameFieldMap.get("decimal(65,-3)");
        assertEquals("double(68,0)", decimal650Field.getDataType());

        TapField decimal550Field = nameFieldMap.get("decimal(55,-3)");
        assertEquals("decimal(58,0)", decimal550Field.getDataType());

        TapField decimal6530Field = nameFieldMap.get("decimal(65,30)");
        assertEquals("decimal(65,30)", decimal6530Field.getDataType());

        TapField floatField = nameFieldMap.get("float");
        assertEquals("float", floatField.getDataType());

        TapField floatUnsignedField = nameFieldMap.get("float unsigned");
        assertEquals("float(6,6) unsigned", floatUnsignedField.getDataType());

        TapField float8Field = nameFieldMap.get("float(8)");
        assertEquals("float", float8Field.getDataType());

        TapField double256UnsignedField = nameFieldMap.get("double(256) unsigned");
        assertEquals("double(77,6) unsigned", double256UnsignedField.getDataType());

        TapField doubleField = nameFieldMap.get("double");
        assertEquals("double(77,6)", doubleField.getDataType());

        TapField bigint150Field = nameFieldMap.get("bigint(150)");
        assertEquals("superbigint", bigint150Field.getDataType());

        TapField bigint50Field = nameFieldMap.get("bigint(50)");
        assertEquals("bigint", bigint50Field.getDataType());
    }

    @Test
    void stringTest() {
        String sourceTypeExpression = "{\n" +
                "    \"varchar[($byte)]\": {\"byte\": \"64k\", \"fixed\": false, \"to\": \"TapString\", \"defaultByte\": 1},\n" +
                "    \"longtext\": {\"byte\": \"4g\", \"to\": \"TapString\"},\n" +
                "    \"superlongtext\": {\"byte\": \"8g\", \"to\": \"TapString\"},\n" +
                "    \"char[($byte)]\": {\"byte\": 255, \"to\": \"TapString\", \"byteRatio\": 3, \"fixed\": true, },\n" +
                "    \"tinytext\": {\"byte\": 255, \"to\": \"TapString\"},\n" +
                "    \"text\": {\"byte\": \"64k\", \"to\": \"TapString\"},\n" +
                "    \"mediumtext\": {\"byte\": \"16m\", \"to\": \"TapString\"},\n" +

                "    \"int[($bit)][unsigned][zerofill]\": {\"bit\": 32, \"bitRatio\": 3, \"unsigned\": \"unsigned\", \"zerofill\": \"zerofill\", \"to\": \"TapNumber\"},\n" +
                "    \"decimal($precision,$scale)[theUnsigned][theZerofill]\": {\"precision\":[1, 65], \"scale\": [-3, 30], \"unsigned\": \"theUnsigned\", \"zerofill\": \"theZerofill\", \"precisionDefault\": 10, \"scaleDefault\": 0, \"to\": \"TapNumber\"},\n" +
                "    \"tinyint[($bit)][unsigned][zerofill]\": {\"bit\": 1, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"smallint[($bit)][unsigned][zerofill]\": {\"bit\": 4, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"mediumint[($bit)][unsigned][zerofill]\": {\"bit\": 8, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"bigint($bit)[unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"bigint[unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"float[($bit)][unsigned][zerofill]\": {\"bit\": 16, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"double[($bit)][unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"date\": {\"range\": [\"1000-01-01\", \"9999-12-31\"], \"pattern\": \"yyyy-MM-dd\", \"to\": \"TapDate\"},\n" +
                "    \"time\": {\"range\": [\"-838:59:59\",\"838:59:59\"], \"to\": \"TapTime\"},\n" +
                "    \"year\": {\"range\": [1901, 2155], \"to\": \"TapYear\"},\n" +
                "    \"datetime\": {\"range\": [\"1000-01-01 00:00:00\", \"9999-12-31 23:59:59\"], \"pattern\": \"yyyy-MM-dd HH:mm:ss\", \"to\": \"TapDateTime\"},\n" +
                "    \"timestamp\": {\"to\": \"TapDateTime\"},\n" +

                "    \"longblob\": {\"byte\": \"4g\", \"to\": \"TapBinary\"},\n" +
                "    \"tinyblob\": {\"byte\": 255, \"to\": \"TapBinary\"},\n" +
                "    \"blob\": {\"byte\": \"64k\", \"to\": \"TapBinary\"},\n" +
                "    \"mediumblob\": {\"byte\": \"16m\", \"to\": \"TapBinary\"},\n" +
                "    \"bit($byte)\": {\"byte\": 8, \"to\": \"TapBinary\"},\n" +
                "    \"varbinary($byte)\": {\"byte\": 255, \"fixed\": false, \"to\": \"TapBinary\"},\n" +
                "    \"binary($byte)\": {\"byte\": 255, \"to\": \"TapBinary\"},\n" +
                "    \"[varbinary]($byte)[ABC$hi]aaaa[DDD[AAA|BBB]]\": {\"byte\": 33333, \"fixed\": false, \"to\": \"TapBinary\"}\n" +
                "}";
        String targetTypeExpression = "{" +
                "\"char[($byte)]\": {\"to\": \"TapString\",\"byte\": 255, \"byteRatio\": 3, \"defaultByte\": 1,\"fixed\": true},\n" +
                "\"varchar($byte)\": {\"to\": \"TapString\",\"byte\": 65535,\"defaultByte\": 1},\n" +
                "\"tinytext\": {\"to\": \"TapString\",\"byte\": 255},\n" +
                "\"text\": {\"to\": \"TapString\",\"byte\": \"64k\"},\n" +
                "\"mediumtext\": {\"to\": \"TapString\",\"byte\": \"16m\"},\n" +
                "\"longtext\": {\"to\": \"TapString\",\"byte\": \"4g\"},\n" +
                "\"json\": {\"to\": \"TapMap\",\"byte\": \"4g\",\"queryOnly\": true},\n" +
                "\"binary[($byte)]\": {\"to\": \"TapBinary\",\"byte\": 255,\"defaultByte\": 1,\"fixed\": true},\n" +
                "\"varbinary[($byte)]\": {\"to\": \"TapBinary\",\"byte\": 65535,\"defaultByte\": 1},\n" +
                "\"tinyblob\": {\"to\": \"TapBinary\",\"byte\": 255},\n" +
                "\"blob\": {\"to\": \"TapBinary\",\"byte\": \"64k\"},\n" +
                "\"mediumblob\": {\"to\": \"TapBinary\",\"byte\": \"16m\"},\n" +
                "\"longblob\": {\"to\": \"TapBinary\",\"byte\": \"4g\"},\n" +
                "\"bit[($bit)]\": {\"to\": \"TapNumber\",\"bit\": 64,\"precision\": 20,\"value\": [ 0, 18446744073709552000]},\n" +
                "\"tinyint\": {\"to\": \"TapNumber\",\"bit\": 8,\"precision\": 3,\"value\": [ 0, 255]},\n" +
                "\"tinyint unsigned\": {\"to\": \"TapNumber\",\"bit\": 8,\"precision\": 3,\"value\": [ -128, 127],\"unsigned\": \"unsigned\"},\n" +
                "\"smallint\": {\"to\": \"TapNumber\",\"bit\": 16,\"value\": [ -32768, 32767],\"precision\": 5},\n" +
                "\"smallint unsigned\": {\"to\": \"TapNumber\",\"bit\": 16,\"precision\": 5,\"value\": [ 0, 65535],\"unsigned\": \"unsigned\"},\n" +
                "\"mediumint\": {\"to\": \"TapNumber\",\"bit\": 24,\"precision\": 7,\"value\": [ -8388608, 8388607]},\n" +
                "\"mediumint unsigned\": {\"to\": \"TapNumber\",\"bit\": 24,\"precision\": 8,\"value\": [ 0, 16777215],\"unsigned\": \"unsigned\"},\n" +
                "\"int\": {\"to\": \"TapNumber\",\"bit\": 32,\"precision\": 10,\"value\": [ -2147483648, 2147483647]},\n" +
                "\"int unsigned\": {\"to\": \"TapNumber\",\"bit\": 32,\"precision\": 10,\"value\": [ 0, 4294967295]},\n" +
                "\"bigint\": {\"to\": \"TapNumber\",\"bit\": 64,\"precision\": 19,\"value\": [ -9223372036854775808, 9223372036854775807]},\n" +
                "\"bigint unsigned\": {\"to\": \"TapNumber\",\"bit\": 64,\"precision\": 20,\"value\": [ 0, 18446744073709551615], \"unsigned\": \"unsigned\"},\n" +
                "\"decimal[($precision,$scale)][unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 65],\"scale\": [ 0, 30],\"defaultPrecision\": 10,\"defaultScale\": 0,\"unsigned\": \"unsigned\", \"fixed\": true},\n" +
                "\"float($precision,$scale)[unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 30],\"scale\": [ 0, 30],\"value\": [ \"-3.402823466E+38\", \"3.402823466E+38\"],\"unsigned\": \"unsigned\",\"fixed\": false},\n" +
                "\"float\": {\"to\": \"TapNumber\",\"precision\": [ 1, 6],\"scale\": [ 0, 6],\"fixed\": false},\n" +
                "\"double\": {\"to\": \"TapNumber\",\"precision\": [ 1, 11],\"scale\": [ 0, 11],\"fixed\": false},\n" +
                "\"double[($precision,$scale)][unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 255],\"scale\": [ 0, 30],\"value\": [ \"-1.7976931348623157E+308\", \"1.7976931348623157E+308\"],\"unsigned\": \"unsigned\",\"fixed\": false},\n" +
                "\"date\": {\"to\": \"TapDate\",\"range\": [ \"1000-01-01\", \"9999-12-31\"],\"pattern\": \"yyyy-MM-dd\"},\n" +
                "\"time\": {\"to\": \"TapTime\",\"range\": [ \"-838:59:59\", \"838:59:59\"]},\n" +
                "\"datetime[($precision)]\": {\"to\": \"TapDateTime\",\"range\": [ \"1000-01-01 00:00:00.000000\", \"9999-12-31 23:59:59.999999\"],\"pattern\": \"yyyy-MM-dd HH:mm:ss.SSSSSS\",\"precision\": [ 0, 6],\"defaultPrecision\": 0},\n" +
                "\"timestamp[($precision)]\": {\"to\": \"TapDateTime\",\"range\": [ \"1970-01-01 00:00:01.000000\", \"2038-01-19 03:14:07.999999\"],\"pattern\": \"yyyy-MM-dd HH:mm:ss.SSSSSS\",\"precision\": [ 0, 6],\"defaultPrecision\": 0,\"withTimezone\": true}\n"
                + "}";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("varchar(400)", "varchar(400)"))
                .add(field("varchar(40)", "varchar(40)"))
                .add(field("char(30)", "char(30)"))
                .add(field("tinytext", "tinytext"))
                .add(field("text", "text"))
                .add(field("longtext", "longtext"))
                .add(field("superlongtext", "superlongtext"))
                .add(field("varchar(64k)", "varchar(64k)"))
                .add(field("varchar", "varchar"))

        ;
        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();

        TapField varchar400Field = nameFieldMap.get("varchar(400)");
        assertEquals("varchar(400)", varchar400Field.getDataType());

        TapField varchar40Field = nameFieldMap.get("varchar(40)");
        assertEquals("varchar(40)", varchar40Field.getDataType());

        TapField char30Field = nameFieldMap.get("char(30)");
        assertEquals("char(30)", char30Field.getDataType());

        TapField tinytextField = nameFieldMap.get("tinytext");
        assertEquals("varchar(255)", tinytextField.getDataType());

        TapField textField = nameFieldMap.get("text");
        assertEquals("varchar(65535)", textField.getDataType());

        TapField longtextField = nameFieldMap.get("longtext");
        assertEquals("longtext", longtextField.getDataType());

        TapField superlongtextField = nameFieldMap.get("superlongtext");
        assertEquals("longtext", superlongtextField.getDataType());

        TapField varchar64kField = nameFieldMap.get("varchar(64k)");
        assertEquals("varchar(65535)", varchar64kField.getDataType());

        TapField varcharField = nameFieldMap.get("varchar");
        assertEquals("varchar(1)", varcharField.getDataType());
    }

    @Test
    void binaryTest() {
        String sourceTypeExpression = "{\n" +
                "    \"varchar[($byte)]\": {\"byte\": \"64k\", \"fixed\": false, \"to\": \"TapString\", \"defaultByte\": 1},\n" +
                "    \"longtext\": {\"byte\": \"4g\", \"to\": \"TapString\"},\n" +
                "    \"superlongtext\": {\"byte\": \"8g\", \"to\": \"TapString\"},\n" +
                "    \"char[($byte)]\": {\"byte\": 255, \"to\": \"TapString\", \"byteRatio\": 3, \"fixed\": true, },\n" +
                "    \"tinytext\": {\"byte\": 255, \"to\": \"TapString\"},\n" +
                "    \"text\": {\"byte\": \"64k\", \"to\": \"TapString\"},\n" +
                "    \"mediumtext\": {\"byte\": \"16m\", \"to\": \"TapString\"},\n" +

                "    \"int[($bit)][unsigned][zerofill]\": {\"bit\": 32, \"bitRatio\": 3, \"unsigned\": \"unsigned\", \"zerofill\": \"zerofill\", \"to\": \"TapNumber\"},\n" +
                "    \"decimal($precision,$scale)[theUnsigned][theZerofill]\": {\"precision\":[1, 65], \"scale\": [-3, 30], \"unsigned\": \"theUnsigned\", \"zerofill\": \"theZerofill\", \"precisionDefault\": 10, \"scaleDefault\": 0, \"to\": \"TapNumber\"},\n" +
                "    \"tinyint[($bit)][unsigned][zerofill]\": {\"bit\": 1, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"smallint[($bit)][unsigned][zerofill]\": {\"bit\": 4, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"mediumint[($bit)][unsigned][zerofill]\": {\"bit\": 8, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"bigint($bit)[unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"bigint[unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"float[($bit)][unsigned][zerofill]\": {\"bit\": 16, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"double[($bit)][unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"date\": {\"range\": [\"1000-01-01\", \"9999-12-31\"], \"gmt\": 8, \"to\": \"TapDate\"},\n" +
                "    \"time\": {\"range\": [\"-838:59:59\",\"838:59:59\"], \"gmt\": 8, \"to\": \"TapTime\"},\n" +
                "    \"year\": {\"range\": [1901, 2155], \"to\": \"TapYear\"},\n" +
                "    \"datetime\": {\"range\": [\"1000-01-01 00:00:00\", \"9999-12-31 23:59:59\"], \"pattern\": \"yyyy-MM-dd HH:mm:ss\", \"to\": \"TapDateTime\"},\n" +
                "    \"timestamp\": {\"to\": \"TapDateTime\"},\n" +

                "    \"longblob\": {\"byte\": \"4g\", \"to\": \"TapBinary\"},\n" +
                "    \"tinyblob\": {\"byte\": 255, \"to\": \"TapBinary\"},\n" +
                "    \"blob\": {\"byte\": \"64k\", \"to\": \"TapBinary\"},\n" +
                "    \"mediumblob\": {\"byte\": \"16m\", \"to\": \"TapBinary\"},\n" +
                "    \"bit($byte)\": {\"byte\": 8, \"to\": \"TapBinary\"},\n" +
                "    \"varbinary($byte)\": {\"byte\": 255, \"fixed\": false, \"to\": \"TapBinary\"},\n" +
                "    \"binary($byte)\": {\"byte\": 255, \"to\": \"TapBinary\"},\n" +
                "    \"[varbinary]($byte)[ABC$hi]aaaa[DDD[AAA|BBB]]\": {\"byte\": 33333, \"fixed\": false, \"to\": \"TapBinary\"}\n" +
                "}";
        String targetTypeExpression = "{" +
                "\"char[($byte)]\": {\"to\": \"TapString\",\"byte\": 255, \"byteRatio\": 3, \"defaultByte\": 1,\"fixed\": true},\n" +
                "\"varchar($byte)\": {\"to\": \"TapString\",\"byte\": 65535,\"defaultByte\": 1},\n" +
                "\"tinytext\": {\"to\": \"TapString\",\"byte\": 255},\n" +
                "\"text\": {\"to\": \"TapString\",\"byte\": \"64k\"},\n" +
                "\"mediumtext\": {\"to\": \"TapString\",\"byte\": \"16m\"},\n" +
                "\"longtext\": {\"to\": \"TapString\",\"byte\": \"4g\"},\n" +
                "\"json\": {\"to\": \"TapMap\",\"byte\": \"4g\",\"queryOnly\": true},\n" +
                "\"binary[($byte)]\": {\"to\": \"TapBinary\",\"byte\": 255,\"defaultByte\": 1,\"fixed\": true},\n" +
                "\"varbinary[($byte)]\": {\"to\": \"TapBinary\",\"byte\": 65535,\"defaultByte\": 1},\n" +
                "\"tinyblob\": {\"to\": \"TapBinary\",\"byte\": 255},\n" +
                "\"blob\": {\"to\": \"TapBinary\",\"byte\": \"64k\"},\n" +
                "\"mediumblob\": {\"to\": \"TapBinary\",\"byte\": \"16m\"},\n" +
                "\"longblob\": {\"to\": \"TapBinary\",\"byte\": \"4g\"},\n" +
                "\"bit[($bit)]\": {\"to\": \"TapNumber\",\"bit\": 64,\"precision\": 20,\"value\": [ 0, 18446744073709552000]},\n" +
                "\"tinyint\": {\"to\": \"TapNumber\",\"bit\": 8,\"precision\": 3,\"value\": [ 0, 255]},\n" +
                "\"tinyint unsigned\": {\"to\": \"TapNumber\",\"bit\": 8,\"precision\": 3,\"value\": [ -128, 127],\"unsigned\": \"unsigned\"},\n" +
                "\"smallint\": {\"to\": \"TapNumber\",\"bit\": 16,\"value\": [ -32768, 32767],\"precision\": 5},\n" +
                "\"smallint unsigned\": {\"to\": \"TapNumber\",\"bit\": 16,\"precision\": 5,\"value\": [ 0, 65535],\"unsigned\": \"unsigned\"},\n" +
                "\"mediumint\": {\"to\": \"TapNumber\",\"bit\": 24,\"precision\": 7,\"value\": [ -8388608, 8388607]},\n" +
                "\"mediumint unsigned\": {\"to\": \"TapNumber\",\"bit\": 24,\"precision\": 8,\"value\": [ 0, 16777215],\"unsigned\": \"unsigned\"},\n" +
                "\"int\": {\"to\": \"TapNumber\",\"bit\": 32,\"precision\": 10,\"value\": [ -2147483648, 2147483647]},\n" +
                "\"int unsigned\": {\"to\": \"TapNumber\",\"bit\": 32,\"precision\": 10,\"value\": [ 0, 4294967295]},\n" +
                "\"bigint\": {\"to\": \"TapNumber\",\"bit\": 64,\"precision\": 19,\"value\": [ -9223372036854775808, 9223372036854775807]},\n" +
                "\"bigint unsigned\": {\"to\": \"TapNumber\",\"bit\": 64,\"precision\": 20,\"value\": [ 0, 18446744073709551615], \"unsigned\": \"unsigned\"},\n" +
                "\"decimal[($precision,$scale)][unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 65],\"scale\": [ 0, 30],\"defaultPrecision\": 10,\"defaultScale\": 0,\"unsigned\": \"unsigned\", \"fixed\": true},\n" +
                "\"float($precision,$scale)[unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 30],\"scale\": [ 0, 30],\"value\": [ \"-3.402823466E+38\", \"3.402823466E+38\"],\"unsigned\": \"unsigned\",\"fixed\": false},\n" +
                "\"float\": {\"to\": \"TapNumber\",\"precision\": [ 1, 6],\"scale\": [ 0, 6],\"fixed\": false},\n" +
                "\"double\": {\"to\": \"TapNumber\",\"precision\": [ 1, 11],\"scale\": [ 0, 11],\"fixed\": false},\n" +
                "\"double[($precision,$scale)][unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 255],\"scale\": [ 0, 30],\"value\": [ \"-1.7976931348623157E+308\", \"1.7976931348623157E+308\"],\"unsigned\": \"unsigned\",\"fixed\": false},\n" +
                "\"date\": {\"to\": \"TapDate\",\"range\": [ \"1000-01-01\", \"9999-12-31\"],\"pattern\": \"yyyy-MM-dd\"},\n" +
                "\"time\": {\"to\": \"TapTime\",\"range\": [ \"-838:59:59\", \"838:59:59\"]},\n" +
                "\"datetime[($fraction)]\": {\"to\": \"TapDateTime\",\"range\": [ \"1000-01-01 00:00:00.000000\", \"9999-12-31 23:59:59.999999\"],\"pattern\": \"yyyy-MM-dd HH:mm:ss.SSSSSS\",\"fraction\": [ 0, 6],\"defaultFraction\": 0},\n" +
                "\"timestamp[($fraction)]\": {\"to\": \"TapDateTime\",\"range\": [ \"1970-01-01 00:00:01.000000\", \"2038-01-19 03:14:07.999999\"],\"pattern\": \"yyyy-MM-dd HH:mm:ss.SSSSSS\",\"fraction\": [ 0, 6],\"defaultFraction\": 0,\"withTimezone\": true}\n"
                + "}";

        TapTable sourceTable = table("test");
        sourceTable
//        "    \"longblob\": {\"byte\": \"4g\", \"to\": \"TapBinary\"},\n" +
//                "    \"tinyblob\": {\"byte\": 255, \"to\": \"TapBinary\"},\n" +
//                "    \"blob\": {\"byte\": \"64k\", \"to\": \"TapBinary\"},\n" +
//                "    \"mediumblob\": {\"byte\": \"16m\", \"to\": \"TapBinary\"},\n" +
//                "    \"bit($byte)\": {\"byte\": 8, \"to\": \"TapBinary\"},\n" +
//                "    \"varbinary($byte)\": {\"byte\": 255, \"fixed\": false, \"to\": \"TapBinary\"},\n" +
//                "    \"binary($byte)\": {\"byte\": 255, \"to\": \"TapBinary\"},\n" +
                .add(field("longblob", "longblob"))
                .add(field("tinyblob", "tinyblob"))
                .add(field("blob", "blob"))
                .add(field("mediumblob", "mediumblob"))
                .add(field("bit(8)", "bit(8)"))
                .add(field("varbinary(200)", "varbinary(200)"))
                .add(field("binary(100)", "binary(100)"))

        ;
        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();

        TapField longblobField = nameFieldMap.get("longblob");
        assertEquals("longblob", longblobField.getDataType());

        TapField tinyblobField = nameFieldMap.get("tinyblob");
        assertEquals("varbinary(255)", tinyblobField.getDataType());

        TapField blobField = nameFieldMap.get("blob");
        assertEquals("varbinary(65535)", blobField.getDataType());

        TapField mediumblobField = nameFieldMap.get("mediumblob");
        assertEquals("mediumblob", mediumblobField.getDataType());

        TapField bit8Field = nameFieldMap.get("bit(8)");
        assertEquals("varbinary(8)", bit8Field.getDataType());

        TapField varbinary200Field = nameFieldMap.get("varbinary(200)");
        assertEquals("varbinary(200)", varbinary200Field.getDataType());

        TapField binary100Field = nameFieldMap.get("binary(100)");
        assertEquals("varbinary(100)", binary100Field.getDataType());

    }


//    @Test
//    public void tddSourceTest() {
//        String sourceTypeExpression = "{\n" +
//                    "\"tapString[($byte)][fixed]\": {\"byte\" : \"16m\", \"fixed\" : \"fixed\", \"to\" : \"TapString\"},\n" +
//                    "\"tapNumber[($precision, $scale)]\": {\"precision\" : [1, 40], \"precisionDefault\" : 4, \"scale\" : [0, 10], \"scaleDefault\" : 1, \"to\": \"TapNumber\"},\n" +
//                    "\"int[($bit)][unsigned][zerofill]\": {\"bit\": 32, \"bitDefault\" : 32, \"unsigned\": \"unsigned\", \"zerofill\" :  \"zerofill\", \"to\": \"TapNumber\"},\n" +
//                    "\"tapBoolean\" : {\"bit\": 8, \"to\": \"TapBoolean\"},\n" +
//                    "\"tapDate\" : {\"to\": \"TapDate\"},\n" +
//                    "\"tapArray\" : {\"to\": \"TapArray\"},\n" +
//                    "\"tapRaw\" : {\"to\": \"TapRaw\"},\n" +
//                    "\"tapBinary\" : {\"to\": \"TapBinary\"},\n" +
//                    "\"tapMap\" : {\"to\": \"TapMap\"},\n" +
//                    "\"tapTime\" : {\"to\": \"TapTime\"},\n" +
//                    "\"tapDateTime\" : {\"to\": \"TapDateTime\"}"
//                + "}";
//        String targetTypeExpression = "{" +
//                "\"char[($byte)]\": {\"to\": \"TapString\",\"byte\": 255, \"byteRatio\": 3, \"defaultByte\": 1,\"fixed\": true},\n" +
//                "\"varchar($byte)\": {\"to\": \"TapString\",\"byte\": 65535,\"defaultByte\": 1},\n" +
//                "\"tinytext\": {\"to\": \"TapString\",\"byte\": 255},\n" +
//                "\"text\": {\"to\": \"TapString\",\"byte\": \"64k\"},\n" +
//                "\"mediumtext\": {\"to\": \"TapString\",\"byte\": \"16m\"},\n" +
//                "\"longtext\": {\"to\": \"TapString\",\"byte\": \"4g\"},\n" +
//                "\"json\": {\"to\": \"TapMap\",\"byte\": \"4g\",\"queryOnly\": true},\n" +
//                "\"binary[($byte)]\": {\"to\": \"TapBinary\",\"byte\": 255,\"defaultByte\": 1,\"fixed\": true},\n" +
//                "\"varbinary[($byte)]\": {\"to\": \"TapBinary\",\"byte\": 65535,\"defaultByte\": 1},\n" +
//                "\"tinyblob\": {\"to\": \"TapBinary\",\"byte\": 255},\n" +
//                "\"blob\": {\"to\": \"TapBinary\",\"byte\": \"64k\"},\n" +
//                "\"mediumblob\": {\"to\": \"TapBinary\",\"byte\": \"16m\"},\n" +
//                "\"longblob\": {\"to\": \"TapBinary\",\"byte\": \"4g\"},\n" +
//                "\"bit[($bit)]\": {\"to\": \"TapNumber\",\"bit\": 64,\"precision\": 20,\"value\": [ 0, 18446744073709552000]},\n" +
//                "\"tinyint\": {\"to\": \"TapNumber\",\"bit\": 8,\"precision\": 3,\"value\": [ 0, 255]},\n" +
//                "\"tinyint unsigned\": {\"to\": \"TapNumber\",\"bit\": 8,\"precision\": 3,\"value\": [ -128, 127],\"unsigned\": \"unsigned\"},\n" +
//                "\"smallint\": {\"to\": \"TapNumber\",\"bit\": 16,\"value\": [ -32768, 32767],\"precision\": 5},\n" +
//                "\"smallint unsigned\": {\"to\": \"TapNumber\",\"bit\": 16,\"precision\": 5,\"value\": [ 0, 65535],\"unsigned\": \"unsigned\"},\n" +
//                "\"mediumint\": {\"to\": \"TapNumber\",\"bit\": 24,\"precision\": 7,\"value\": [ -8388608, 8388607]},\n" +
//                "\"mediumint unsigned\": {\"to\": \"TapNumber\",\"bit\": 24,\"precision\": 8,\"value\": [ 0, 16777215],\"unsigned\": \"unsigned\"},\n" +
//                "\"int\": {\"to\": \"TapNumber\",\"bit\": 32,\"precision\": 10,\"value\": [ -2147483648, 2147483647]},\n" +
//                "\"int unsigned\": {\"to\": \"TapNumber\",\"bit\": 32,\"precision\": 10, \"unsigned\": \"unsigned\", \"value\": [ 0, 4294967295]},\n" +
//                "\"bigint\": {\"to\": \"TapNumber\",\"bit\": 64,\"precision\": 19,\"value\": [ -9223372036854775808, 9223372036854775807]},\n" +
//                "\"bigint unsigned\": {\"to\": \"TapNumber\",\"bit\": 64,\"precision\": 20,\"value\": [ 0, 18446744073709551615], \"unsigned\": \"unsigned\"},\n" +
//                "\"decimal[($precision,$scale)][unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 65],\"scale\": [ 0, 30],\"defaultPrecision\": 10,\"defaultScale\": 0,\"unsigned\": \"unsigned\", \"fixed\": true},\n" +
//                "\"float($precision,$scale)[unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 30],\"scale\": [ 0, 30],\"value\": [ \"-3.402823466E+38\", \"3.402823466E+38\"],\"unsigned\": \"unsigned\",\"fixed\": false},\n" +
//                "\"float\": {\"to\": \"TapNumber\",\"precision\": [ 1, 6],\"scale\": [ 0, 6],\"fixed\": false},\n" +
//                "\"double\": {\"to\": \"TapNumber\",\"precision\": [ 1, 11],\"scale\": [ 0, 11],\"fixed\": false},\n" +
//                "\"double[($precision,$scale)][unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 255],\"scale\": [ 0, 30],\"value\": [ \"-1.7976931348623157E+308\", \"1.7976931348623157E+308\"],\"unsigned\": \"unsigned\",\"fixed\": false},\n" +
//                "\"date\": {\"to\": \"TapDate\",\"range\": [ \"1000-01-01\", \"9999-12-31\"],\"pattern\": \"yyyy-MM-dd\"},\n" +
//                "\"time\": {\"to\": \"TapTime\",\"range\": [ \"-838:59:59\", \"838:59:59\"]},\n" +
//                "\"datetime[($fraction)]\": {\"to\": \"TapDateTime\",\"range\": [ \"1000-01-01 00:00:00.000000\", \"9999-12-31 23:59:59.999999\"],\"pattern\": \"yyyy-MM-dd HH:mm:ss.SSSSSS\",\"fraction\": [ 0, 6],\"defaultFraction\": 0},\n" +
//                "\"timestamp[($fraction)]\": {\"to\": \"TapDateTime\",\"range\": [ \"1970-01-01 00:00:01.000000\", \"2038-01-19 03:14:07.999999\"],\"pattern\": \"yyyy-MM-dd HH:mm:ss.SSSSSS\",\"fraction\": [ 0, 6],\"defaultFraction\": 0,\"withTimeZone\": true}\n"
//                + "}";
//
//        TapTable sourceTable = table("test");
//        sourceTable
////        "\"tapString[($byte)][fixed]\": {\"byte\" : \"16m\", \"fixed\" : \"fixed\", \"to\" : \"TapString\"},\n" +
////                "\"tapNumber[($precision, $scale)]\": {\"precision\" : [1, 40], \"precisionDefault\" : 4, \"scale\" : [0, 10], \"scaleDefault\" : 1, \"to\": \"TapNumber\"},\n" +
////                "\"int[($bit)][unsigned][zerofill]\": {\"bit\": 32, \"bitDefault\" : 32, \"unsigned\": \"unsigned\", \"zerofill\" :  \"zerofill\", \"to\": \"TapNumber\"},\n" +
////                "\"tapBoolean\" : {\"to\": \"TapBoolean\"},\n" +
////                "\"tapDate\" : {\"to\": \"TapDate\"},\n" +
////                "\"tapArray\" : {\"to\": \"TapArray\"},\n" +
////                "\"tapRaw\" : {\"to\": \"TapRaw\"},\n" +
////                "\"tapBinary\" : {\"to\": \"TapBinary\"},\n" +
////                "\"tapMap\" : {\"to\": \"TapMap\"},\n" +
////                "\"tapTime\" : {\"to\": \"TapTime\"},\n" +
////                "\"tapDateTime\" : {\"to\": \"TapDateTime\"}"
//                .add(field("tapString", "tapString"))
//                .add(field("tapString(64k)", "tapString(64k)"))
//                .add(field("tapString(64)", "tapString(64)"))
//                .add(field("tapNumber", "tapNumber"))
//                .add(field("tapNumber(20, 3)", "tapNumber(20, 3)"))
//                .add(field("int", "int"))
//                .add(field("int unsigned", "int unsigned"))
//                .add(field("int(32)", "int(32)"))
//                .add(field("tapBoolean", "tapBoolean"))
//                .add(field("tapDateTime", "tapDateTime"))
//
//        ;
//        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
//        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);
//
//        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();
//
//    }

    @Test
    public void dateTimeTest() {
        String sourceTypeExpression = "{\n" +
                "\"tapString[($byte)][fixed]\": {\"byte\" : \"16m\", \"fixed\" : \"fixed\", \"to\" : \"TapString\"},\n" +
                "\"tapNumber[($precision, $scale)]\": {\"precision\" : [1, 40], \"precisionDefault\" : 4, \"scale\" : [0, 10], \"scaleDefault\" : 1, \"to\": \"TapNumber\"},\n" +
                "\"int[($bit)][unsigned][zerofill]\": {\"bit\": 32, \"bitDefault\" : 32, \"unsigned\": \"unsigned\", \"zerofill\" :  \"zerofill\", \"to\": \"TapNumber\"},\n" +
                "\"tapBoolean\" : {\"bit\": 8, \"to\": \"TapBoolean\"},\n" +
                "\"tapDate\" : {\"to\": \"TapDate\"},\n" +
                "\"tapArray\" : {\"to\": \"TapArray\"},\n" +
                "\"tapRaw\" : {\"to\": \"TapRaw\"},\n" +
                "\"tapBinary\" : {\"to\": \"TapBinary\"},\n" +
                "\"tapMap\" : {\"to\": \"TapMap\"},\n" +
                "\"tapTime\" : {\"to\": \"TapTime\"},\n" +
                "\"dateTimeWithTZ[($fraction)]\": {\"to\": \"TapDateTime\",\"range\": [ \"1970-01-01 00:00:01.000000\", \"2038-01-19 03:14:07.999999\"],\"pattern\": \"yyyy-MM-dd HH:mm:ss.SSSSSS\",\"fraction\": [ 0, 6],\"defaultFraction\": 0,\"withTimeZone\": true},\n" +
                "\"dateTime\" : {\"to\": \"TapDateTime\", \"range\": [ \"1970-01-01 00:00:01.000000000\", \"2058-01-19 03:14:07.999999999\"],\"pattern\": \"yyyy-MM-dd HH:mm:ss.SSSSSSSSS\",\"fraction\": [ 0, 9],\"defaultFraction\": 3}"
                + "}";
        String targetTypeExpression = "{" +
                "\"char[($byte)]\": {\"to\": \"TapString\",\"byte\": 255, \"byteRatio\": 3, \"defaultByte\": 1,\"fixed\": true},\n" +
                "\"varchar($byte)\": {\"to\": \"TapString\",\"byte\": 65535,\"defaultByte\": 1},\n" +
                "\"tinytext\": {\"to\": \"TapString\",\"byte\": 255},\n" +
                "\"text\": {\"to\": \"TapString\",\"byte\": \"64k\"},\n" +
                "\"mediumtext\": {\"to\": \"TapString\",\"byte\": \"16m\"},\n" +
                "\"longtext\": {\"to\": \"TapString\",\"byte\": \"4g\"},\n" +
                "\"json\": {\"to\": \"TapMap\",\"byte\": \"4g\",\"queryOnly\": true},\n" +
                "\"binary[($byte)]\": {\"to\": \"TapBinary\",\"byte\": 255,\"defaultByte\": 1,\"fixed\": true},\n" +
                "\"varbinary[($byte)]\": {\"to\": \"TapBinary\",\"byte\": 65535,\"defaultByte\": 1},\n" +
                "\"tinyblob\": {\"to\": \"TapBinary\",\"byte\": 255},\n" +
                "\"blob\": {\"to\": \"TapBinary\",\"byte\": \"64k\"},\n" +
                "\"mediumblob\": {\"to\": \"TapBinary\",\"byte\": \"16m\"},\n" +
                "\"longblob\": {\"to\": \"TapBinary\",\"byte\": \"4g\"},\n" +
                "\"bit[($bit)]\": {\"to\": \"TapNumber\",\"bit\": 64,\"precision\": 20,\"value\": [ 0, 18446744073709552000]},\n" +
                "\"tinyint[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 8,\"precision\": 3,\"value\": [ -128, 127],\"unsignedValue\": [ 0, 255],\"unsigned\": \"unsigned\"},\n" +
                "\"smallint[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 16,\"value\": [ -32768, 32767],\"unsignedValue\": [ 0, 65535],\"unsigned\": \"unsigned\",\"precision\": 5},\n" +
                "\"mediumint[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 24,\"precision\": 7,\"value\": [ -8388608, 8388607],\"unsignedValue\": [ 0, 16777215],\"unsigned\": \"unsigned\"},\n" +
                "\"int[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 32,\"precision\": 10,\"value\": [ -2147483648, 2147483647],\"unsignedValue\": [ 0, 4294967295],\"unsigned\": \"unsigned\"},\n" +
                "\"bigint[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 64,\"precision\": 19,\"value\": [ -9223372036854775808, 9223372036854775807], \"unsignedValue\": [ 0, 18446744073709551615],\"unsigned\": \"unsigned\"},\n" +
                "\"decimal[($precision,$scale)][unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 65],\"scale\": [ 0, 30],\"defaultPrecision\": 10,\"defaultScale\": 0,\"unsigned\": \"unsigned\", \"fixed\": true},\n" +
                "\"float($precision,$scale)[unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 30],\"scale\": [ 0, 30],\"value\": [ \"-3.402823466E+38\", \"3.402823466E+38\"],\"unsigned\": \"unsigned\",\"fixed\": false},\n" +
                "\"float\": {\"to\": \"TapNumber\",\"precision\": [ 1, 6],\"scale\": [ 0, 6],\"fixed\": false},\n" +
                "\"double\": {\"to\": \"TapNumber\",\"precision\": [ 1, 11],\"scale\": [ 0, 11],\"fixed\": false},\n" +
                "\"double[($precision,$scale)][unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 255],\"scale\": [ 0, 30],\"value\": [ \"-1.7976931348623157E+308\", \"1.7976931348623157E+308\"],\"unsigned\": \"unsigned\",\"fixed\": false},\n" +
                "\"date\": {\"to\": \"TapDate\",\"range\": [ \"1000-01-01\", \"9999-12-31\"],\"pattern\": \"yyyy-MM-dd\"},\n" +
                "\"time\": {\"to\": \"TapTime\",\"range\": [\"-838:59:59\",\"838:59:59\"]},\n" +
                "\"datetime[($fraction)]\": {\"to\": \"TapDateTime\",\"range\": [ \"1000-01-01 00:00:00.000000\", \"9999-12-31 23:59:59.999999\"],\"pattern\": \"yyyy-MM-dd HH:mm:ss.SSSSSS\",\"fraction\": [ 0, 6],\"defaultFraction\": 0},\n" +
                "\"timestamp[($fraction)]\": {\"to\": \"TapDateTime\",\"range\": [ \"1970-01-01 00:00:01.000000\", \"2038-01-19 03:14:07.999999\"],\"pattern\": \"yyyy-MM-dd HH:mm:ss.SSSSSS\",\"fraction\": [ 0, 6],\"defaultFraction\": 0,\"withTimeZone\": true}\n"
                + "}";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("dateTimeWithTZ(6)", "dateTimeWithTZ(6)"))
                .add(field("dateTimeWithTZ", "dateTimeWithTZ"))
                .add(field("dateTime", "dateTime"))
                .add(field("tapDate", "tapDate"))
                .add(field("tapTime", "tapTime"))

        ;
        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();

        TapField dateTimeWithTZ6Field = nameFieldMap.get("dateTimeWithTZ(6)");
        assertEquals("timestamp(6)", dateTimeWithTZ6Field.getDataType());

        TapField dateTimeWithTZField = nameFieldMap.get("dateTimeWithTZ");
        assertEquals("timestamp(0)", dateTimeWithTZField.getDataType());

        TapField dateTimeField = nameFieldMap.get("dateTime");
        assertEquals("datetime(3)", dateTimeField.getDataType());
    }

    @Test
    public void ObjectIdTest() {
        String sourceTypeExpression = "{\n" +
                "\"ObjectId\": {\"byte\" : \"24\", \"fixed\": true, \"to\" : \"TapString\"},\n"
                + "}";
        String targetTypeExpression = "{" +
                "\"char[($byte)]\": {\"to\": \"TapString\",\"byte\": 255, \"defaultByte\": 1,\"fixed\": true},\n" +
                "\"varchar($byte)\": {\"to\": \"TapString\",\"byte\": 16383,\"defaultByte\": 4, \"byteRatio\": 4},\n" +
                "\"tinytext\": {\"to\": \"TapString\",\"byte\": 255},\n" +
                "\"text\": {\"to\": \"TapString\",\"byte\": \"64k\"},\n" +
                "\"mediumtext\": {\"to\": \"TapString\",\"byte\": \"16m\"},\n" +
                "\"longtext\": {\"to\": \"TapString\",\"byte\": \"4g\"},\n" +
                "\"json\": {\"to\": \"TapMap\",\"byte\": \"4g\",\"queryOnly\": true},\n" +
                "\"binary[($byte)]\": {\"to\": \"TapBinary\",\"byte\": 255,\"defaultByte\": 1,\"fixed\": true},\n" +
                "\"varbinary[($byte)]\": {\"to\": \"TapBinary\",\"byte\": 65535,\"defaultByte\": 1},\n" +
                "\"tinyblob\": {\"to\": \"TapBinary\",\"byte\": 255},\n" +
                "\"blob\": {\"to\": \"TapBinary\",\"byte\": \"64k\"},\n" +
                "\"mediumblob\": {\"to\": \"TapBinary\",\"byte\": \"16m\"},\n" +
                "\"longblob\": {\"to\": \"TapBinary\",\"byte\": \"4g\"},\n" +
                "\"bit[($bit)]\": {\"to\": \"TapNumber\",\"bit\": 64,\"precision\": 20,\"value\": [ 0, 18446744073709552000]},\n" +
                "\"tinyint[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 8,\"precision\": 3,\"value\": [ -128, 127],\"unsignedValue\": [ 0, 255],\"unsigned\": \"unsigned\"},\n" +
                "\"smallint[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 16,\"value\": [ -32768, 32767],\"unsignedValue\": [ 0, 65535],\"unsigned\": \"unsigned\",\"precision\": 5},\n" +
                "\"mediumint[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 24,\"precision\": 7,\"value\": [ -8388608, 8388607],\"unsignedValue\": [ 0, 16777215],\"unsigned\": \"unsigned\"},\n" +
                "\"int[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 32,\"precision\": 10,\"value\": [ -2147483648, 2147483647],\"unsignedValue\": [ 0, 4294967295],\"unsigned\": \"unsigned\"},\n" +
                "\"bigint[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 64,\"precision\": 19,\"value\": [ -9223372036854775808, 9223372036854775807], \"unsignedValue\": [ 0, 18446744073709551615],\"unsigned\": \"unsigned\"},\n" +
                "\"decimal[($precision,$scale)][unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 65],\"scale\": [ 0, 30],\"defaultPrecision\": 10,\"defaultScale\": 0,\"unsigned\": \"unsigned\", \"fixed\": true},\n" +
                "\"float($precision,$scale)[unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 30],\"scale\": [ 0, 30],\"value\": [ \"-3.402823466E+38\", \"3.402823466E+38\"],\"unsigned\": \"unsigned\",\"fixed\": false},\n" +
                "\"float\": {\"to\": \"TapNumber\",\"precision\": [ 1, 6],\"scale\": [ 0, 6],\"fixed\": false},\n" +
                "\"double\": {\"to\": \"TapNumber\",\"precision\": [ 1, 11],\"scale\": [ 0, 11],\"fixed\": false},\n" +
                "\"double[($precision,$scale)][unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 255],\"scale\": [ 0, 30],\"value\": [ \"-1.7976931348623157E+308\", \"1.7976931348623157E+308\"],\"unsigned\": \"unsigned\",\"fixed\": false},\n" +
                "\"date\": {\"to\": \"TapDate\",\"range\": [ \"1000-01-01\", \"9999-12-31\"],\"pattern\": \"yyyy-MM-dd\"},\n" +
                "\"time\": {\"to\": \"TapTime\",\"range\": [\"-838:59:59\",\"838:59:59\"]},\n" +
                "\"datetime[($fraction)]\": {\"to\": \"TapDateTime\",\"range\": [ \"1000-01-01 00:00:00.000000\", \"9999-12-31 23:59:59.999999\"],\"pattern\": \"yyyy-MM-dd HH:mm:ss.SSSSSS\",\"fraction\": [ 0, 6],\"defaultFraction\": 0},\n" +
                "\"timestamp[($fraction)]\": {\"to\": \"TapDateTime\",\"range\": [ \"1970-01-01 00:00:01.000000\", \"2038-01-19 03:14:07.999999\"],\"pattern\": \"yyyy-MM-dd HH:mm:ss.SSSSSS\",\"fraction\": [ 0, 6],\"defaultFraction\": 0,\"withTimeZone\": true}\n"
                + "}";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("ObjectId", "ObjectId"))

        ;
        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();

        TapField ObjectIdField = nameFieldMap.get("ObjectId");
        assertEquals("char(24)", ObjectIdField.getDataType());

    }

    @Test
    public void intTest() {
        String targetTypeExpression = "{\n" +
                "\"text\": {\"to\": \"TapString\",\"byte\": \"64k\"},\n" +
                "\"intxxx\": {\"to\": \"TapNumber\", \"bit\": 32,\n"
                + "}";
        String sourceTypeExpression = "{" +
                "\"char[($byte)]\": {\"to\": \"TapString\",\"byte\": 255, \"defaultByte\": 1,\"fixed\": true},\n" +
                "\"varchar($byte)\": {\"to\": \"TapString\",\"byte\": 16383,\"defaultByte\": 4, \"byteRatio\": 4},\n" +
                "\"tinytext\": {\"to\": \"TapString\",\"byte\": 255},\n" +
                "\"text\": {\"to\": \"TapString\",\"byte\": \"64k\"},\n" +
                "\"mediumtext\": {\"to\": \"TapString\",\"byte\": \"16m\"},\n" +
                "\"longtext\": {\"to\": \"TapString\",\"byte\": \"4g\"},\n" +
                "\"json\": {\"to\": \"TapMap\",\"byte\": \"4g\",\"queryOnly\": true},\n" +
                "\"binary[($byte)]\": {\"to\": \"TapBinary\",\"byte\": 255,\"defaultByte\": 1,\"fixed\": true},\n" +
                "\"varbinary[($byte)]\": {\"to\": \"TapBinary\",\"byte\": 65535,\"defaultByte\": 1},\n" +
                "\"tinyblob\": {\"to\": \"TapBinary\",\"byte\": 255},\n" +
                "\"blob\": {\"to\": \"TapBinary\",\"byte\": \"64k\"},\n" +
                "\"mediumblob\": {\"to\": \"TapBinary\",\"byte\": \"16m\"},\n" +
                "\"longblob\": {\"to\": \"TapBinary\",\"byte\": \"4g\"},\n" +
                "\"bit[($bit)]\": {\"to\": \"TapNumber\",\"bit\": 64,\"precision\": 20,\"value\": [ 0, 18446744073709552000]},\n" +
                "\"tinyint[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 8,\"precision\": 3,\"value\": [ -128, 127],\"unsignedValue\": [ 0, 255],\"unsigned\": \"unsigned\"},\n" +
                "\"smallint[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 16,\"value\": [ -32768, 32767],\"unsignedValue\": [ 0, 65535],\"unsigned\": \"unsigned\",\"precision\": 5},\n" +
                "\"mediumint[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 24,\"precision\": 7,\"value\": [ -8388608, 8388607],\"unsignedValue\": [ 0, 16777215],\"unsigned\": \"unsigned\"},\n" +
                "\"int[($zerofill)]\": {\"to\": \"TapNumber\", \"bit\": 32, \"precision\": 10, \"value\": [-2147483648, 2147483647]},\n" +
                "\"bigint[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 64,\"precision\": 19,\"value\": [ -9223372036854775808, 9223372036854775807], \"unsignedValue\": [ 0, 18446744073709551615],\"unsigned\": \"unsigned\"},\n" +
                "\"decimal[($precision,$scale)][unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 65],\"scale\": [ 0, 30],\"defaultPrecision\": 10,\"defaultScale\": 0,\"unsigned\": \"unsigned\", \"fixed\": true},\n" +
                "\"float($precision,$scale)[unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 30],\"scale\": [ 0, 30],\"value\": [ \"-3.402823466E+38\", \"3.402823466E+38\"],\"unsigned\": \"unsigned\",\"fixed\": false},\n" +
                "\"float\": {\"to\": \"TapNumber\",\"precision\": [ 1, 6],\"scale\": [ 0, 6],\"fixed\": false},\n" +
                "\"double\": {\"to\": \"TapNumber\",\"precision\": [ 1, 11],\"scale\": [ 0, 11],\"fixed\": false},\n" +
                "\"double[($precision,$scale)][unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 255],\"scale\": [ 0, 30],\"value\": [ \"-1.7976931348623157E+308\", \"1.7976931348623157E+308\"],\"unsigned\": \"unsigned\",\"fixed\": false},\n" +
                "\"date\": {\"to\": \"TapDate\",\"range\": [ \"1000-01-01\", \"9999-12-31\"],\"pattern\": \"yyyy-MM-dd\"},\n" +
                "\"time\": {\"to\": \"TapTime\",\"range\": [\"-838:59:59\",\"838:59:59\"]},\n" +
                "\"datetime[($fraction)]\": {\"to\": \"TapDateTime\",\"range\": [ \"1000-01-01 00:00:00.000000\", \"9999-12-31 23:59:59.999999\"],\"pattern\": \"yyyy-MM-dd HH:mm:ss.SSSSSS\",\"fraction\": [ 0, 6],\"defaultFraction\": 0},\n" +
                "\"timestamp[($fraction)]\": {\"to\": \"TapDateTime\",\"range\": [ \"1970-01-01 00:00:01.000000\", \"2038-01-19 03:14:07.999999\"],\"pattern\": \"yyyy-MM-dd HH:mm:ss.SSSSSS\",\"fraction\": [ 0, 6],\"defaultFraction\": 0,\"withTimeZone\": true}\n"
                + "}";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("int(11)", "int(11)"))

        ;
        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();

        TapField ObjectIdField = nameFieldMap.get("int(11)");
        assertEquals("intxxx", ObjectIdField.getDataType());

    }
    @Test
    public void preferTest() {
        String sourceTypeExpression = "{" +
                "\"char[($byte)]\": {\"to\": \"TapString\",\"byte\": 255, \"preferByte\": 10, \"defaultByte\": 1,\"fixed\": true},\n" +
                "\"decimal[($precision,$scale)][unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 65],\"scale\": [ 0, 30],\"defaultPrecision\": 10, \"preferPrecision\": 3,\"defaultScale\": 0,\"preferScale\": 3,\"unsigned\": \"unsigned\", \"fixed\": true},\n" +
                "\"bit[($bit)]\": {\"to\": \"TapNumber\",\"bit\": 64,\"precision\": 20,\"preferBit\": 20,\"value\": [ 0, 18446744073709552000]},\n" +
                "\"varchar($byte)\": {\"to\": \"TapString\",\"byte\": 16383,\"defaultByte\": 4, \"byteRatio\": 4},\n" +
                "\"tinytext\": {\"to\": \"TapString\",\"byte\": 255},\n" +
                "\"text\": {\"to\": \"TapString\",\"byte\": \"64k\"},\n" +
                "\"mediumtext\": {\"to\": \"TapString\",\"byte\": \"16m\"},\n" +
                "\"longtext\": {\"to\": \"TapString\",\"byte\": \"4g\"},\n" +
                "\"json\": {\"to\": \"TapMap\",\"byte\": \"4g\",\"queryOnly\": true},\n" +
                "\"binary[($byte)]\": {\"to\": \"TapBinary\",\"byte\": 255,\"defaultByte\": 1,\"fixed\": true},\n" +
                "\"varbinary[($byte)]\": {\"to\": \"TapBinary\",\"byte\": 65535,\"defaultByte\": 1},\n" +
                "\"tinyblob\": {\"to\": \"TapBinary\",\"byte\": 255},\n" +
                "\"blob\": {\"to\": \"TapBinary\",\"byte\": \"64k\"},\n" +
                "\"mediumblob\": {\"to\": \"TapBinary\",\"byte\": \"16m\"},\n" +
                "\"longblob\": {\"to\": \"TapBinary\",\"byte\": \"4g\"},\n" +
                "\"tinyint[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 8,\"precision\": 3,\"value\": [ -128, 127],\"unsignedValue\": [ 0, 255],\"unsigned\": \"unsigned\"},\n" +
                "\"smallint[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 16,\"value\": [ -32768, 32767],\"unsignedValue\": [ 0, 65535],\"unsigned\": \"unsigned\",\"precision\": 5},\n" +
                "\"mediumint[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 24,\"precision\": 7,\"value\": [ -8388608, 8388607],\"unsignedValue\": [ 0, 16777215],\"unsigned\": \"unsigned\"},\n" +
                "\"int[($zerofill)]\": {\"to\": \"TapNumber\", \"bit\": 32, \"precision\": 10, \"value\": [-2147483648, 2147483647]},\n" +
                "\"bigint[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 64,\"precision\": 19,\"value\": [ -9223372036854775808, 9223372036854775807], \"unsignedValue\": [ 0, 18446744073709551615],\"unsigned\": \"unsigned\"},\n" +
                "\"float($precision,$scale)[unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 30],\"scale\": [ 0, 30],\"value\": [ \"-3.402823466E+38\", \"3.402823466E+38\"],\"unsigned\": \"unsigned\",\"fixed\": false},\n" +
                "\"float\": {\"to\": \"TapNumber\",\"precision\": [ 1, 6],\"scale\": [ 0, 6],\"fixed\": false},\n" +
                "\"double\": {\"to\": \"TapNumber\",\"precision\": [ 1, 11],\"scale\": [ 0, 11],\"fixed\": false},\n" +
                "\"double[($precision,$scale)][unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 255],\"scale\": [ 0, 30],\"value\": [ \"-1.7976931348623157E+308\", \"1.7976931348623157E+308\"],\"unsigned\": \"unsigned\",\"fixed\": false},\n" +
                "\"date\": {\"to\": \"TapDate\",\"range\": [ \"1000-01-01\", \"9999-12-31\"],\"pattern\": \"yyyy-MM-dd\"},\n" +
                "\"time\": {\"to\": \"TapTime\",\"range\": [\"-838:59:59\",\"838:59:59\"]},\n" +
                "\"datetime[($fraction)]\": {\"to\": \"TapDateTime\",\"range\": [ \"1000-01-01 00:00:00.000000\", \"9999-12-31 23:59:59.999999\"],\"pattern\": \"yyyy-MM-dd HH:mm:ss.SSSSSS\",\"fraction\": [ 0, 6],\"defaultFraction\": 0},\n" +
                "\"timestamp[($fraction)]\": {\"to\": \"TapDateTime\",\"range\": [ \"1970-01-01 00:00:01.000000\", \"2038-01-19 03:14:07.999999\"],\"pattern\": \"yyyy-MM-dd HH:mm:ss.SSSSSS\",\"fraction\": [ 0, 6],\"defaultFraction\": 0,\"withTimeZone\": true}\n"
                + "}";
        String targetTypeExpression = "{\n" +
                "    \"char[($byte)]\":{\"byte\":255, \"byteRatio\": 2, \"to\": \"TapString\", \"defaultByte\": 1},\n" +
                "    \"decimal[($precision,$scale)]\":{\"precision\": [1, 27], \"defaultPrecision\": 10, \"scale\": [0, 9], \"defaultScale\": 0, \"to\": \"TapNumber\"},\n" +
                "    \"string\":{\"byte\":\"2147483643\", \"to\":\"TapString\"},\n" +
                "    \"myint[($bit)][unsigned]\":{\"bit\":24, \"unsigned\":\"unsigned\", \"to\":\"TapNumber\"},\n" +

                "    \"largeint\":{\"bit\":128, \"to\":\"TapNumber\"},\n" +
                "    \"boolean\":{\"bit\":8, \"unsigned\":\"\", \"to\":\"TapNumber\"},\n" +
                "    \"tinyint\":{\"bit\":8, \"to\":\"TapNumber\"},\n" +
                "    \"smallint\":{\"bit\":16, \"to\":\"TapNumber\"},\n" +
                "    \"int\":{\"bit\":32, \"to\":\"TapNumber\"},\n" +
                "    \"bigint\":{\"bit\":64, \"to\":\"TapNumber\"},\n" +
                "    \"float\":{\"bit\":32, \"to\":\"TapNumber\"},\n" +
                "    \"double\":{\"bit\":64, \"to\":\"TapNumber\"},\n" +
                "    \"date\":{\"byte\":3, \"range\":[\"1000-01-01\", \"9999-12-31\"], \"to\":\"TapDate\"},\n" +
                "    \"datetime\":{\"byte\":8, \"range\":[\"1000-01-01 00:00:00\",\"9999-12-31 23:59:59\"],\"pattern\": \"yyyy-MM-dd HH:mm:ss\", \"to\":\"TapDateTime\"},\n" +
                "    \"varchar[($byte)]\":{\"byte\":\"65535\", \"to\":\"TapString\"},\n" +
                "    \"HLL\":{\"byte\":\"16385\", \"to\":\"TapNumber\", \"queryOnly\":true}\n" +
                "}";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("char", "char"))
                .add(field("decimal", "decimal"))
                .add(field("bit", "bit"))

        ;
        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();


        TapField charField = nameFieldMap.get("char");
        assertEquals("char(10)", charField.getDataType());

        TapField decimalField = nameFieldMap.get("decimal");
        assertEquals("decimal(3,3)", decimalField.getDataType());

        TapField bitField = nameFieldMap.get("bit");
        assertEquals("myint(20)", bitField.getDataType());

    }


    @Test
    public void pgNumericTest() {
        String sourceTypeExpression = "{\n" +
                "    \"numeric[($precision,$scale)]\": {\"precision\": [1,1000],\"scale\": [0,1000],\"fixed\": false,\"preferPrecision\": 20,\"preferScale\": 8,\"priority\": 1,\"to\": \"TapNumber\"}\n" +
                "}";

        String targetTypeExpression = "{" +
//                "\"char[($byte)]\": {\"to\": \"TapString\",\"byte\": 255, \"preferByte\": 10, \"defaultByte\": 1,\"fixed\": true},\n" +
                "\"decimal[($precision,$scale)][unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 65],\"scale\": [ 0, 30],\"defaultPrecision\": 10, \"preferPrecision\": 3,\"defaultScale\": 0,\"preferScale\": 3,\"unsigned\": \"unsigned\", \"fixed\": true},\n" +
//                "\"bit[($bit)]\": {\"to\": \"TapNumber\",\"bit\": 64,\"precision\": 20,\"preferBit\": 20,\"value\": [ 0, 18446744073709552000]},\n" +
//                "\"varchar($byte)\": {\"to\": \"TapString\",\"byte\": 16383,\"defaultByte\": 4, \"byteRatio\": 4},\n" +
//                "\"tinytext\": {\"to\": \"TapString\",\"byte\": 255},\n" +
//                "\"text\": {\"to\": \"TapString\",\"byte\": \"64k\"},\n" +
//                "\"mediumtext\": {\"to\": \"TapString\",\"byte\": \"16m\"},\n" +
//                "\"longtext\": {\"to\": \"TapString\",\"byte\": \"4g\"},\n" +
//                "\"json\": {\"to\": \"TapMap\",\"byte\": \"4g\",\"queryOnly\": true},\n" +
//                "\"binary[($byte)]\": {\"to\": \"TapBinary\",\"byte\": 255,\"defaultByte\": 1,\"fixed\": true},\n" +
//                "\"varbinary[($byte)]\": {\"to\": \"TapBinary\",\"byte\": 65535,\"defaultByte\": 1},\n" +
//                "\"tinyblob\": {\"to\": \"TapBinary\",\"byte\": 255},\n" +
//                "\"blob\": {\"to\": \"TapBinary\",\"byte\": \"64k\"},\n" +
//                "\"mediumblob\": {\"to\": \"TapBinary\",\"byte\": \"16m\"},\n" +
//                "\"longblob\": {\"to\": \"TapBinary\",\"byte\": \"4g\"},\n" +
//                "\"tinyint[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 8,\"precision\": 3,\"value\": [ -128, 127],\"unsignedValue\": [ 0, 255],\"unsigned\": \"unsigned\"},\n" +
//                "\"smallint[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 16,\"value\": [ -32768, 32767],\"unsignedValue\": [ 0, 65535],\"unsigned\": \"unsigned\",\"precision\": 5},\n" +
//                "\"mediumint[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 24,\"precision\": 7,\"value\": [ -8388608, 8388607],\"unsignedValue\": [ 0, 16777215],\"unsigned\": \"unsigned\"},\n" +
//                "\"int[($zerofill)]\": {\"to\": \"TapNumber\", \"bit\": 32, \"precision\": 10, \"value\": [-2147483648, 2147483647]},\n" +
//                "\"bigint[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 64,\"precision\": 19,\"value\": [ -9223372036854775808, 9223372036854775807], \"unsignedValue\": [ 0, 18446744073709551615],\"unsigned\": \"unsigned\"},\n" +
                "\"float($precision,$scale)[unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 30],\"scale\": [ 0, 30],\"value\": [ \"-3.402823466E+38\", \"3.402823466E+38\"],\"unsigned\": \"unsigned\",\"fixed\": false},\n" +
                "\"float\": {\"to\": \"TapNumber\",\"precision\": [ 1, 6],\"scale\": [ 0, 6],\"fixed\": false},\n" +
                "\"double\": {\"to\": \"TapNumber\",\"precision\": [ 1, 11],\"scale\": [ 0, 11],\"fixed\": false},\n" +
                "\"double[($precision,$scale)][unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 255],\"defaultPrecision\": 10,\"scale\": [ 0, 30],\"value\": [ \"-1.7976931348623157E+308\", \"1.7976931348623157E+308\"],\"unsigned\": \"unsigned\",\"fixed\": false},\n" +
//                "\"date\": {\"to\": \"TapDate\",\"range\": [ \"1000-01-01\", \"9999-12-31\"],\"pattern\": \"yyyy-MM-dd\"},\n" +
//                "\"time\": {\"to\": \"TapTime\",\"range\": [\"-838:59:59\",\"838:59:59\"]},\n" +
//                "\"datetime[($fraction)]\": {\"to\": \"TapDateTime\",\"range\": [ \"1000-01-01 00:00:00.000000\", \"9999-12-31 23:59:59.999999\"],\"pattern\": \"yyyy-MM-dd HH:mm:ss.SSSSSS\",\"fraction\": [ 0, 6],\"defaultFraction\": 0},\n" +
                "\"timestamp[($fraction)]\": {\"to\": \"TapDateTime\",\"range\": [ \"1970-01-01 00:00:01.000000\", \"2038-01-19 03:14:07.999999\"],\"pattern\": \"yyyy-MM-dd HH:mm:ss.SSSSSS\",\"fraction\": [ 0, 6],\"defaultFraction\": 0,\"withTimeZone\": true}\n"
                + "}";


        TapTable sourceTable = table("test");
        sourceTable
                .add(field("numeric(1000,0)", "numeric(1000,0)"))
                .add(field("numeric", "numeric"))
        ;
        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();


        TapField numeric1000_0Field = nameFieldMap.get("numeric(1000,0)");
        assertEquals("double(255,0)", numeric1000_0Field.getDataType());

        TapField decimalField = nameFieldMap.get("numeric");
        assertEquals("float(20,8)", decimalField.getDataType());
    }

    @Test
    public void mongodbDecimal128Test() {

        String sourceTypeExpression = "{\n" +
                "   \"DECIMAL128\": {\"to\": \"TapNumber\",\"value\": [-1E+6145,1E+6145]} \n" +
                "}";

        String targetTypeExpression = "{" +
//                "\"char[($byte)]\": {\"to\": \"TapString\",\"byte\": 255, \"preferByte\": 10, \"defaultByte\": 1,\"fixed\": true},\n" +
                "\"decimal[($precision,$scale)][unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 65],\"scale\": [ 0, 30],\"defaultPrecision\": 10, \"preferPrecision\": 3,\"defaultScale\": 0,\"preferScale\": 3,\"unsigned\": \"unsigned\", \"fixed\": true},\n" +
//                "\"bit[($bit)]\": {\"to\": \"TapNumber\",\"bit\": 64,\"precision\": 20,\"preferBit\": 20,\"value\": [ 0, 18446744073709552000]},\n" +
//                "\"varchar($byte)\": {\"to\": \"TapString\",\"byte\": 16383,\"defaultByte\": 4, \"byteRatio\": 4},\n" +
//                "\"tinytext\": {\"to\": \"TapString\",\"byte\": 255},\n" +
//                "\"text\": {\"to\": \"TapString\",\"byte\": \"64k\"},\n" +
//                "\"mediumtext\": {\"to\": \"TapString\",\"byte\": \"16m\"},\n" +
//                "\"longtext\": {\"to\": \"TapString\",\"byte\": \"4g\"},\n" +
//                "\"json\": {\"to\": \"TapMap\",\"byte\": \"4g\",\"queryOnly\": true},\n" +
//                "\"binary[($byte)]\": {\"to\": \"TapBinary\",\"byte\": 255,\"defaultByte\": 1,\"fixed\": true},\n" +
//                "\"varbinary[($byte)]\": {\"to\": \"TapBinary\",\"byte\": 65535,\"defaultByte\": 1},\n" +
//                "\"tinyblob\": {\"to\": \"TapBinary\",\"byte\": 255},\n" +
//                "\"blob\": {\"to\": \"TapBinary\",\"byte\": \"64k\"},\n" +
//                "\"mediumblob\": {\"to\": \"TapBinary\",\"byte\": \"16m\"},\n" +
//                "\"longblob\": {\"to\": \"TapBinary\",\"byte\": \"4g\"},\n" +
//                "\"tinyint[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 8,\"precision\": 3,\"value\": [ -128, 127],\"unsignedValue\": [ 0, 255],\"unsigned\": \"unsigned\"},\n" +
//                "\"smallint[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 16,\"value\": [ -32768, 32767],\"unsignedValue\": [ 0, 65535],\"unsigned\": \"unsigned\",\"precision\": 5},\n" +
//                "\"mediumint[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 24,\"precision\": 7,\"value\": [ -8388608, 8388607],\"unsignedValue\": [ 0, 16777215],\"unsigned\": \"unsigned\"},\n" +
//                "\"int[($zerofill)]\": {\"to\": \"TapNumber\", \"bit\": 32, \"precision\": 10, \"value\": [-2147483648, 2147483647]},\n" +
//                "\"bigint[unsigned]\": {\"to\": \"TapNumber\",\"bit\": 64,\"precision\": 19,\"value\": [ -9223372036854775808, 9223372036854775807], \"unsignedValue\": [ 0, 18446744073709551615],\"unsigned\": \"unsigned\"},\n" +
                "\"float($precision,$scale)[unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 30],\"scale\": [ 0, 30],\"value\": [ \"-3.402823466E+38\", \"3.402823466E+38\"],\"unsigned\": \"unsigned\",\"fixed\": false},\n" +
                "\"float\": {\"to\": \"TapNumber\",\"precision\": [ 1, 6],\"scale\": [ 0, 6],\"fixed\": false},\n" +
                "\"double\": {\"to\": \"TapNumber\",\"precision\": [ 1, 11],\"scale\": [ 0, 11],\"fixed\": false},\n" +
                "\"double[($precision,$scale)][unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 255],\"defaultPrecision\": 10,\"scale\": [ 0, 30],\"value\": [ \"-1.7976931348623157E+308\", \"1.7976931348623157E+308\"],\"unsigned\": \"unsigned\",\"fixed\": false},\n" +
//                "\"date\": {\"to\": \"TapDate\",\"range\": [ \"1000-01-01\", \"9999-12-31\"],\"pattern\": \"yyyy-MM-dd\"},\n" +
//                "\"time\": {\"to\": \"TapTime\",\"range\": [\"-838:59:59\",\"838:59:59\"]},\n" +
//                "\"datetime[($fraction)]\": {\"to\": \"TapDateTime\",\"range\": [ \"1000-01-01 00:00:00.000000\", \"9999-12-31 23:59:59.999999\"],\"pattern\": \"yyyy-MM-dd HH:mm:ss.SSSSSS\",\"fraction\": [ 0, 6],\"defaultFraction\": 0},\n" +
                "\"timestamp[($fraction)]\": {\"to\": \"TapDateTime\",\"range\": [ \"1970-01-01 00:00:01.000000\", \"2038-01-19 03:14:07.999999\"],\"pattern\": \"yyyy-MM-dd HH:mm:ss.SSSSSS\",\"fraction\": [ 0, 6],\"defaultFraction\": 0,\"withTimeZone\": true}\n"
                + "}";


        TapTable sourceTable = table("test");
        sourceTable
                .add(field("DECIMAL128", "DECIMAL128"))

        ;
        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();


        TapField numeric1000_0Field = nameFieldMap.get("DECIMAL128");
        assertEquals("double(255,0)", numeric1000_0Field.getDataType());

    }

    @Test
    public void mongoDoubleToPGNumericTest() {
        String sourceTypeExpression = "{\n" +
                "    \"DOUBLE\": {\"to\": \"TapNumber\",\"value\": [\"-1.7976931348623157E+308\",\"1.7976931348623157E+308\"],\"scale\": 17,\"precision\": 309}" +
                "}";

        String targetTypeExpression = "{" +
                "    \"numeric[($precision,$scale)]\": {\"precision\": [1,1000],\"scale\": [0,1000],\"fixed\": false,\"preferPrecision\": 20,\"preferScale\": 8,\"priority\": 1,\"to\": \"TapNumber\"}\n"
                + "}";


        TapTable sourceTable = table("test");
        sourceTable
                .add(field("double", "double"))
        ;
        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();


        TapField doubleField = nameFieldMap.get("double");
        assertEquals("numeric(309,17)", doubleField.getDataType());
    }

    @Test
    public void MySQLDoubleTomongoDoubleTest() {

        String sourceTypeExpression = "{" +
                " \"double\": {\"to\": \"TapNumber\",\"precision\": [1,11],\"scale\": [0,11],\"fixed\": false}"
                + "}";

        String targetTypeExpression = "{\n" +
                "    \"DOUBLE\": {\"to\": \"TapNumber\",\"value\": [\"-1.7976931348623157E+308\",\"1.7976931348623157E+308\"],\"scale\": 17,\"precision\": 309}," +
                "\"DECIMAL128\": {\"to\": \"TapNumber\",\"value\": [-1E+6145,1E+6145],\"scale\": 1000}" +
                "}";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("double", "double"))
        ;
        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();


        TapField doubleField = nameFieldMap.get("double");
        assertEquals("DOUBLE", doubleField.getDataType());
    }

    @Test
    public void ScaleLargerThanPrecisionTest() {

        String sourceTypeExpression = "{" +
                "    \"numeric[($precision,$scale)]\": {\"precision\": [1,1000],\"scale\": [0,1000],\"fixed\": false,\"preferPrecision\": 20,\"preferScale\": 8,\"priority\": 1,\"to\": \"TapNumber\"}\n"
                + "}";

        String targetTypeExpression = "{\n" +
                "\"decimal[($precision,$scale)][unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 65],\"scale\": [ 0, 30],\"defaultPrecision\": 10, \"preferPrecision\": 3,\"defaultScale\": 0,\"preferScale\": 3,\"unsigned\": \"unsigned\", \"fixed\": true},\n" +
                "}";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("numeric(10,50)", "numeric(10,50)"))
                .add(field("numeric(10,11)", "numeric(10,11)"))
        ;
        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();


        TapField number1050Field = nameFieldMap.get("numeric(10,50)");
        assertEquals("decimal(30,30)", number1050Field.getDataType());

        TapField number1011Field = nameFieldMap.get("numeric(10,11)");
        assertEquals("decimal(11,11)", number1011Field.getDataType());
    }

    @Test
    public void precisionExceededMakeScale0Test() {

        String sourceTypeExpression = "{" +
                "    \"numeric[($precision,$scale)]\": {\"precision\": [1,1000],\"scale\": [0,1000],\"fixed\": false,\"preferPrecision\": 20,\"preferScale\": 8,\"priority\": 1,\"to\": \"TapNumber\"}\n"
                + "}";

        String targetTypeExpression = "{\n" +
                "\"decimal[($precision,$scale)][unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 65],\"scale\": [ 0, 30],\"defaultPrecision\": 10, \"preferPrecision\": 3,\"defaultScale\": 0,\"preferScale\": 3,\"unsigned\": \"unsigned\", \"fixed\": true},\n" +
                "}";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("numeric(1000,500)", "numeric(1000,500)"))
                .add(field("numeric(66,11)", "numeric(66,11)"))
        ;
        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();


        TapField number1050Field = nameFieldMap.get("numeric(1000,500)");
        assertEquals("decimal(65,0)", number1050Field.getDataType());

        TapField number1011Field = nameFieldMap.get("numeric(66,11)");
        assertEquals("decimal(65,0)", number1011Field.getDataType());
    }

    @Test
    public void scaleNegativeTest() {

        String sourceTypeExpression = "{" +
                "    \"numeric[($precision,$scale)]\": {\"precision\": [1,1000],\"scale\": [-1000,1000],\"fixed\": false,\"preferPrecision\": 20,\"preferScale\": 8,\"priority\": 1,\"to\": \"TapNumber\"}\n"
                + "}";

        String targetTypeExpression = "{\n" +
                "\"decimal[($precision,$scale)][unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 65],\"scale\": [ 0, 30],\"defaultPrecision\": 10, \"preferPrecision\": 3,\"defaultScale\": 0,\"preferScale\": 3,\"unsigned\": \"unsigned\", \"fixed\": true},\n" +
                "}";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("numeric(10,-5)", "numeric(10,-5)"))
                .add(field("numeric(10,-500)", "numeric(10,-500)"))
        ;
        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();


        TapField number1050Field = nameFieldMap.get("numeric(10,-5)");
        assertEquals("decimal(15,0)", number1050Field.getDataType());

        TapField number1011Field = nameFieldMap.get("numeric(10,-500)");
        assertEquals("decimal(65,0)", number1011Field.getDataType());
    }

    @Test
    public void withTimeZoneTest() {

        String sourceTypeExpression = "{" +
                "\"timestamp[($fraction)] without time zone\": {\"range\": [\"1000-01-01 00:00:00\",\"9999-12-31 23:59:59\"],\"pattern\": \"yyyy-MM-dd HH:mm:ss\",\"fraction\": [0,6],\"withTimeZone\": false,\"defaultFraction\": 6,\"priority\": 1,\"to\": \"TapDateTime\"}," +
                "\"timestamp[($fraction)] with time zone\": {\"range\": [\"1000-01-01 00:00:00\",\"9999-12-31 23:59:59\"],\"pattern\": \"yyyy-MM-dd HH:mm:ss\",\"fraction\": [0,6],\"withTimeZone\": true,\"defaultFraction\": 6,\"priority\": 2,\"to\": \"TapDateTime\"}" +
                "}";

        String targetTypeExpression = "{\n" +
                "\"timestampex[($fraction)] without time zone\": {\"range\": [\"1000-01-01 00:00:00\",\"9999-12-31 23:59:59\"],\"pattern\": \"yyyy-MM-dd HH:mm:ss\",\"fraction\": [0,6],\"withTimeZone\": false,\"defaultFraction\": 6,\"priority\": 1,\"to\": \"TapDateTime\"}," +
                "\"timestampex[($fraction)] with time zone\": {\"range\": [\"1000-01-01 00:00:00\",\"9999-12-31 23:59:59\"],\"pattern\": \"yyyy-MM-dd HH:mm:ss\",\"fraction\": [0,6],\"withTimeZone\": true,\"defaultFraction\": 6,\"priority\": 2,\"to\": \"TapDateTime\"}" +
                "}";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("timestamp(3) without time zone", "timestamp(3) without time zone"))
                .add(field("timestamp with time zone", "timestamp with time zone"))
        ;
        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();


        TapField number1050Field = nameFieldMap.get("timestamp(3) without time zone");
        assertEquals("timestampex(3) without time zone", number1050Field.getDataType());

        TapField number1011Field = nameFieldMap.get("timestamp with time zone");
        assertEquals("timestampex(6) with time zone", number1011Field.getDataType());
    }

    @Test
    public void onlyPrecisionNoScaleTest() {
        String sourceTypeExpression = "{" +
                "\"float[($precision)]\": {\"to\": \"TapNumber\",\"byte\": 8,\"value\": [\"-1.79E+308\", \"1.79E+308\"],\"precision\": [1,30],\"scale\": 1,\"fixed\": false}," +
                "\"float1[($precision)]\": {\"to\": \"TapNumber\",\"byte\": 8,\"value\": [\"-1.79E+308\", \"1.79E+308\"],\"precision\": [1,30],\"scale\": true,\"fixed\": false}," +
                "\"float2[($precision)]\": {\"to\": \"TapNumber\",\"byte\": 8,\"value\": [\"-1.79E+308\", \"1.79E+308\"],\"precision\": [1,30],\"scale\": \"true\",\"fixed\": false}" +
                "}";

        String targetTypeExpression = "{\n" +
                "\"floatex[($precision)]\": {\"to\": \"TapNumber\",\"byte\": 8,\"value\": [\"-1.79E+308\", \"1.79E+308\"],\"precision\": [1,30],\"scale\": 1,\"fixed\": false}" +
                "}";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("float(10)", "float(10)"))
                .add(field("float(1)", "float(1)"))
                .add(field("float1(10)", "float1(10)"))
                .add(field("float2(10)", "float2(10)"))
        ;
        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();


        TapField float10Field = nameFieldMap.get("float(10)");
        assertEquals("floatex(10)", float10Field.getDataType());

        TapField float1Field = nameFieldMap.get("float(1)");
        assertEquals("floatex(1)", float1Field.getDataType());

        TapField float110Field = nameFieldMap.get("float1(10)");
        assertEquals("floatex(10)", float110Field.getDataType());

        TapField float210Field = nameFieldMap.get("float2(10)");
        assertEquals("floatex(10)", float210Field.getDataType());
    }

    @Test
    public void intUnsignedForIntTest() {
        String sourceTypeExpression = "{" +
                "\"int unsigned\": {\"to\": \"TapNumber\",\"bit\": 32,\"value\": [\"0\", \"4294967295\"]}" +
                "}";

        String targetTypeExpression = "{\n" +
                "\"int\": {\"to\": \"TapNumber\",\"bit\": 32,\"value\": [\"-2147483648\", \"2147483647\"]}," +
                "\"bigint\": {\"to\": \"TapNumber\",\"bit\": 64,\"value\": [\"-9223372036854775808\", \"9223372036854775807\"]}" +
                "}";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("int unsigned", "int unsigned"))
        ;
        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
        TapResult<LinkedHashMap<String, TapField>> tapResult =
                targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();

        TapField float10Field = nameFieldMap.get("int unsigned");
        assertEquals("bigint", float10Field.getDataType(), "\"int unsigned\" can not fit in \"int\" from target, but \"bigint\" can");
    }

    @Test
    public void pgVarcharToDB2ClobTest() {

        String sourceTypeExpression = "{" +
                "\"character varying[($byte)]\": {\"byte\": 10485760,\"priority\": 1,\"defaultByte\": 10485760,\"preferByte\": 2000,\"to\": \"TapString\"}" +
                "}";

        String targetTypeExpression = "{\n" +
//                "\"int unsigned\": {\"to\": \"TapNumber\",\"byte\": 32,\"value\": [\"0\", \"4294967295\"]}," +
                "\"CLOB[($byte)]\": {\"byte\": \"2147483647\",\"pkEnablement\": false,\"defaultByte\": 1048576,\"priority\": 2,\"to\": \"TapString\"}," +
                "\"VARCHAR($byte)\": {\"byte\": 32672,\"priority\": 1,\"preferByte\": 2000,\"to\": \"TapString\"}" +
                "}";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("character varying(200)", "character varying(200)"))
        ;
        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();


        TapField characterVarying_200 = nameFieldMap.get("character varying(200)");
        assertEquals("VARCHAR(200)", characterVarying_200.getDataType());

    }

    @Test
    public void oracleToDb2TimestampTest() {

        String sourceTypeExpression = "{" +
                "\"TIMESTAMP[($fraction)]\": {\"range\": [\"0001-01-01 00:00:00.000000000\",\"9999-12-31 23:59:59.999999999\"],\"pattern\": \"yyyy-MM-dd HH:mm:ss.SSSSSSSSS\",\"fraction\": [0,12],\"defaultFraction\": 6,\"withTimeZone\": false,\"priority\": 2,\"to\": \"TapDateTime\"}" +
                "}";

        String targetTypeExpression = "{\n" +
//                "\"int unsigned\": {\"to\": \"TapNumber\",\"byte\": 32,\"value\": [\"0\", \"4294967295\"]}," +
                "\"TIMESTAMP[($fraction)]\": {\"range\": [\"1000-01-01 00:00:00.000000000\",\"9999-12-31 23:59:59.999999999\"],\"pattern\": \"yyyy-MM-dd HH:mm:ss.SSSSSSSSS\",\"fraction\": [0,9],\"defaultFraction\": 6,\"withTimeZone\": false,\"priority\": 2,\"to\": \"TapDateTime\"}," +
                "\"DATE\": {\"range\": [\"1000-01-01 00:00:00.000\",\"9999-12-31 23:59:59.999\"],\"defaultFraction\": 3,\"pattern\": \"yyyy-MM-dd HH:mm:ss.SSS\",\"priority\": 1,\"to\": \"TapDateTime\"}" +
                "}";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("TIMESTAMP(6)", "TIMESTAMP(6)"))
        ;
        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();


        TapField timestamp6 = nameFieldMap.get("TIMESTAMP(6)");
        assertEquals("TIMESTAMP(6)", timestamp6.getDataType());
    }

    @Test
    public void db2Varchar50toOracleCLOBTest() {

        String sourceTypeExpression = "{" +
                "\"VARCHAR($byte)\": {\"byte\": 32672,\"priority\": 1,\"preferByte\": 2000,\"to\": \"TapString\"}" +
                "}";

        String targetTypeExpression = "{\n" +
//                "\"int unsigned\": {\"to\": \"TapNumber\",\"byte\": 32,\"value\": [\"0\", \"4294967295\"]}," +
                "\"CLOB\": {\"byte\": \"4g\",\"pkEnablement\": false,\"priority\": 2,\"to\": \"TapString\"}," +
                "\"VARCHAR2[($byte)]\": {\"byte\": 4000,\"priority\": 1,\"preferByte\": 2000,\"to\": \"TapString\"}" +
                "}";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("VARCHAR(50)", "VARCHAR(50)"))
                .add(field("varchar(50)", "varchar(50)"))
                .add(field("varchar(70)", " varchar(70) ")) //space in dataType will be trimmed automatically
        ;
        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();


        TapField upperVarchar50 = nameFieldMap.get("VARCHAR(50)");
        assertEquals("VARCHAR2(50)", upperVarchar50.getDataType());

        TapField lowerVarchar50 = nameFieldMap.get("varchar(50)");
        assertEquals("VARCHAR2(50)", lowerVarchar50.getDataType());

        TapField lowerVarchar70 = nameFieldMap.get("varchar(70)");
        assertEquals("VARCHAR2(70)", lowerVarchar70.getDataType());
    }

    public static void main(String[] args) {
        TargetTypesGenerator targetTypesGenerator = InstanceFactory.instance(TargetTypesGenerator.class);
        if (targetTypesGenerator == null)
            throw new CoreException(PDKRunnerErrorCodes.SOURCE_TARGET_TYPES_GENERATOR_NOT_FOUND, "TargetTypesGenerator's implementation is not found in current classloader");
        TableFieldTypesGenerator tableFieldTypesGenerator = InstanceFactory.instance(TableFieldTypesGenerator.class);
        if (tableFieldTypesGenerator == null)
            throw new CoreException(PDKRunnerErrorCodes.SOURCE_TABLE_FIELD_TYPES_GENERATOR_NOT_FOUND, "TableFieldTypesGenerator's implementation is not found in current classloader");
        TapCodecsRegistry codecRegistry = TapCodecsRegistry.create();
        TapCodecsFilterManager targetCodecFilterManager = TapCodecsFilterManager.create(codecRegistry);

        String sourceTypeExpression = "{\n" +
                "    \"int[($bit)][unsigned][zerofill]\": {\"bit\": 32, \"bitRatio\": 3, \"unsigned\": \"unsigned\", \"zerofill\": \"zerofill\", \"to\": \"TapNumber\"},\n" +
                "    \"varchar[($byte)]\": {\"byte\": \"64k\", \"byteRatio\": 3, \"fixed\": false, \"to\": \"TapString\"},\n" +
                "    \"decimal($precision,$scale)[theUnsigned][theZerofill]\": {\"precision\":[1, 65], \"scale\": [-3, 30], \"unsigned\": \"theUnsigned\", \"zerofill\": \"theZerofill\", \"precisionDefault\": 10, \"scaleDefault\": 0, \"to\": \"TapNumber\"},\n" +
                "    \"longtext\": {\"byte\": \"4g\", \"to\": \"TapString\"},\n" +

                "    \"tinyint[($bit)][unsigned][zerofill]\": {\"bit\": 1, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"smallint[($bit)][unsigned][zerofill]\": {\"bit\": 4, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"mediumint[($bit)][unsigned][zerofill]\": {\"bit\": 8, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"bigint($bit)[unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"bigint[unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"float[($bit)][unsigned][zerofill]\": {\"bit\": 16, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"double[($bit)][unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"date\": {\"range\": [\"1000-01-01\", \"9999-12-31\"], \"gmt\": 8, \"to\": \"TapDate\"},\n" +
                "    \"time\": {\"range\": [\"-838:59:59\",\"838:59:59\"], \"gmt\": 8, \"to\": \"TapTime\"},\n" +
                "    \"year\": {\"range\": [1901, 2155], \"to\": \"TapYear\"},\n" +
                "    \"datetime\": {\"range\": [\"1000-01-01 00:00:00\", \"9999-12-31 23:59:59\"], \"pattern\": \"yyyy-MM-dd HH:mm:ss\", \"to\": \"TapDateTime\"},\n" +
                "    \"timestamp\": {\"to\": \"TapDateTime\"},\n" +
                "    \"char[($byte)]\": {\"byte\": 255, \"to\": \"TapString\"},\n" +
                "    \"tinyblob\": {\"byte\": 255, \"to\": \"TapBinary\"},\n" +
                "    \"tinytext\": {\"byte\": 255, \"to\": \"TapString\"},\n" +
                "    \"blob\": {\"byte\": \"64k\", \"to\": \"TapBinary\"},\n" +
                "    \"text\": {\"byte\": \"64k\", \"to\": \"TapString\"},\n" +
                "    \"mediumblob\": {\"byte\": \"16m\", \"to\": \"TapBinary\"},\n" +
                "    \"mediumtext\": {\"byte\": \"16m\", \"to\": \"TapString\"},\n" +
                "    \"longblob\": {\"byte\": \"4g\", \"to\": \"TapBinary\"},\n" +
                "    \"bit($byte)\": {\"byte\": 8, \"to\": \"TapBinary\"},\n" +
                "    \"binary($byte)\": {\"byte\": 255, \"to\": \"TapBinary\"},\n" +
                "    \"varbinary($byte)\": {\"byte\": 255, \"fixed\": false, \"to\": \"TapBinary\"},\n" +
                "    \"[varbinary]($byte)[ABC$hi]aaaa[DDD[AAA|BBB]]\": {\"byte\": 33333, \"fixed\": false, \"to\": \"TapBinary\"}\n" +
                "}";
        String targetTypeExpression = "{\n" +
                "    \"char[($byte)]\":{\"byte\":255, \"byteRatio\": 2, \"to\": \"TapString\", \"defaultByte\": 1},\n" +
                "    \"decimal[($precision,$scale)]\":{\"precision\": [1, 27], \"defaultPrecision\": 10, \"scale\": [0, 9], \"defaultScale\": 0, \"to\": \"TapNumber\"},\n" +
                "    \"string\":{\"byte\":\"2147483643\", \"to\":\"TapString\"},\n" +
                "    \"myint[($bit)][unsigned]\":{\"bit\":48, \"bitRatio\": 2, \"unsigned\":\"unsigned\", \"to\":\"TapNumber\"},\n" +

                "    \"largeint\":{\"bit\":128, \"to\":\"TapNumber\"},\n" +
                "    \"boolean\":{\"bit\":8, \"unsigned\":\"\", \"to\":\"TapNumber\"},\n" +
                "    \"tinyint\":{\"bit\":8, \"to\":\"TapNumber\"},\n" +
                "    \"smallint\":{\"bit\":16, \"to\":\"TapNumber\"},\n" +
                "    \"int\":{\"bit\":32, \"to\":\"TapNumber\"},\n" +
                "    \"bigint\":{\"bit\":64, \"to\":\"TapNumber\"},\n" +
                "    \"float\":{\"bit\":32, \"to\":\"TapNumber\"},\n" +
                "    \"double\":{\"bit\":64, \"to\":\"TapNumber\"},\n" +
                "    \"date\":{\"byte\":3, \"range\":[\"1000-01-01\", \"9999-12-31\"], \"to\":\"TapDate\"},\n" +
                "    \"datetime\":{\"byte\":8, \"range\":[\"1000-01-01 00:00:00\",\"9999-12-31 23:59:59\"],\"pattern\": \"yyyy-MM-dd HH:mm:ss\", \"to\":\"TapDateTime\"},\n" +
                "    \"varchar[($byte)]\":{\"byte\":\"65535\", \"to\":\"TapString\"},\n" +
                "    \"HLL\":{\"byte\":\"16385\", \"to\":\"TapNumber\", \"queryOnly\":true}\n" +
                "}";

        DefaultExpressionMatchingMap sourceMap = DefaultExpressionMatchingMap.map(sourceTypeExpression);
        DefaultExpressionMatchingMap targetMap = DefaultExpressionMatchingMap.map(targetTypeExpression);

        int count = 10000;
        long time = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            TapTable sourceTable = table("test");
            sourceTable
                    .add(field("int(32) unsigned", "int(32) unsigned"))
                    .add(field("longtext", "longtext")) // exceed the max of target types
                    .add(field("varchar(10)", "varchar(10)"))
                    .add(field("varchar(20)", "varchar(20)"))
                    .add(field("varchar(30)", "varchar(30)"))
                    .add(field("varchar(40)", "varchar(40)"))
                    .add(field("varchar(50)", "varchar(50)"))
                    .add(field("varchar(60)", "varchar(60)"))
                    .add(field("varchar(70)", "varchar(70)"))
                    .add(field("varchar(80)", "varchar(80)"))
                    .add(field("varchar(90)", "varchar(90)"))
                    .add(field("varchar(100)", "varchar(100)"))
                    .add(field("decimal(20, -3)", "decimal(20, -3)"))
            ;
            tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), sourceMap);
            TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), targetMap, targetCodecFilterManager);
        }
        System.out.println("takes " + (System.currentTimeMillis() - time));
    }
    @Test
    public void oracleNumberToMySQLDecimal() {

        String sourceTypeExpression = "{" +
                "\"NUMBER($precision)\": {\"precision\": [1,38],\"scale\": [-84,127],\"fixed\": true,\"preferPrecision\": 20,\"defaultPrecision\": 38,\"preferScale\": 8,\"defaultScale\": 0,\"priority\": 1,\"to\": \"TapNumber\"}," +
                "\"NUMBER[($precision,$scale)]\": {\"precision\": [1,38],\"scale\": [-84,127],\"fixed\": true,\"preferPrecision\": 20,\"defaultPrecision\": 38,\"preferScale\": 8,\"defaultScale\": 0,\"priority\": 1,\"to\": \"TapNumber\"}," +
                "\"NUMBER(*,$scale)\": {\"precision\": [1,38],\"scale\": [-84,127],\"fixed\": true,\"preferPrecision\": 20,\"defaultPrecision\": 38,\"preferScale\": 8,\"defaultScale\": 0,\"priority\": 1,\"to\": \"TapNumber\"}," +
                "}";

        String targetTypeExpression = "{\n" +
//                "\"int unsigned\": {\"to\": \"TapNumber\",\"byte\": 32,\"value\": [\"0\", \"4294967295\"]}," +
                "\"decimal[($precision,$scale)][unsigned]\": {\n" +
                "\t  \"to\": \"TapNumber\",\n" +
                "\t  \"precision\": [\n" +
                "\t\t1,\n" +
                "\t\t65\n" +
                "\t  ],\n" +
                "\t  \"scale\": [\n" +
                "\t\t0,\n" +
                "\t\t30\n" +
                "\t  ],\n" +
                "\t  \"defaultPrecision\": 10,\n" +
                "\t  \"defaultScale\": 0,\n" +
                "\t  \"unsigned\": \"unsigned\",\n" +
                "\t  \"fixed\": true\n" +
                "\t}," +

                "\"float($precision,$scale)[unsigned]\": {\n" +
                "\t  \"to\": \"TapNumber\",\n" +
                "\t  \"name\": \"float\",\n" +
                "\t  \"precision\": [\n" +
                "\t\t1,\n" +
                "\t\t30\n" +
                "\t  ],\n" +
                "\t  \"scale\": [\n" +
                "\t\t0,\n" +
                "\t\t30\n" +
                "\t  ],\n" +
                "\t  \"value\": [\n" +
                "\t\t\"-3.402823466E+38\",\n" +
                "\t\t\"3.402823466E+38\"\n" +
                "\t  ],\n" +
                "\t  \"unsigned\": \"unsigned\",\n" +
                "\t  \"fixed\": false\n" +
                "\t}" +
                "}";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("NUMBER(5,2)", "NUMBER(5,2)"))
                .add(field("NUMBER(*,10)", "NUMBER(*,10)"))
                .add(field("NUMBER(50)", "NUMBER(50)"))
                .add(field("NUMBER(50,*,b)", "NUMBER(50,*,b)"))
        ;

        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();


        TapField number52 = nameFieldMap.get("NUMBER(5,2)");
        assertEquals("decimal(5,2)", number52.getDataType());

        TapField numberStar10 = nameFieldMap.get("NUMBER(*,10)");
        assertEquals("decimal(20,10)", numberStar10.getDataType());

        TapField number50 = nameFieldMap.get("NUMBER(50)");
        assertEquals("decimal(50,8)", number50.getDataType());

        TapField number50StarB = nameFieldMap.get("NUMBER(50,*,b)");
        assertEquals(null, number50StarB.getDataType());
    }


    @Test
    public void tapNumber52ToPGNumber() {

        String sourceTypeExpression = "{\"tapNumber[($precision, $scale)]\": {\"precision\" : [1, 40], \"fixed\" : true, \"defaultPrecision\" : 4, \"scale\" : [0, 10], \"defaultScale\" : 1, \"to\": \"TapNumber\"}}";

        String targetTypeExpression = "{\"numeric[($precision,$scale)]\": {\n" +
                "      \"precision\": [\n" +
                "        1,\n" +
                "        1000\n" +
                "      ],\n" +
                "      \"scale\": [\n" +
                "        0,\n" +
                "        1000\n" +
                "      ],\n" +
                "      \"fixed\": true,\n" +
                "      \"preferPrecision\": 20,\n" +
                "      \"preferScale\": 8,\n" +
                "      \"priority\": 1,\n" +
                "      \"to\": \"TapNumber\"\n" +
                "    },\n" +
                "    \"real\": {\n" +
                "      \"bit\": 32,\n" +
                "      \"priority\": 2,\n" +
                "      \"precision\": [\n" +
                "        1,\n" +
                "        6\n" +
                "      ],\n" +
                "      \"scale\": [\n" +
                "        0,\n" +
                "        6\n" +
                "      ],\n" +
//                "       \"value\": [ \"-3.402823466E+38\", \"3.402823466E+38\"]," +
                "      \"fixed\": false,\n" +
                "      \"to\": \"TapNumber\"\n" +
                "    }" +
                "}";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("tapNumber(5, 2)", "tapNumber(5, 2)"))
                .add(field("tapNumber(20, -3)", "tapNumber(20, -3)"))
        ;

        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();

        TapField upperVarchar50 = nameFieldMap.get("tapNumber(5, 2)");
        assertEquals("numeric(5,2)", upperVarchar50.getDataType());

        TapField number20m3 = nameFieldMap.get("tapNumber(20, -3)");
        assertEquals("numeric(23,0)", number20m3.getDataType());
    }

    @Test
    public void tapNumberMinusToMinus() {

        String sourceTypeExpression = "{\"tapNumber[($precision, $scale)]\": {\"precision\" : [1, 40], \"fixed\" : true, \"precisionDefault\" : 4, \"scale\" : [0, 10], \"scaleDefault\" : 1, \"to\": \"TapNumber\"}}";

        String targetTypeExpression = "{\"numeric[($precision,$scale)]\": {\n" +
                "      \"precision\": [\n" +
                "        1,\n" +
                "        1000\n" +
                "      ],\n" +
                "      \"scale\": [\n" +
                "        -100,\n" +
                "        1000\n" +
                "      ],\n" +
                "      \"fixed\": true,\n" +
                "      \"preferPrecision\": 20,\n" +
                "      \"preferScale\": 8,\n" +
                "      \"priority\": 1,\n" +
                "      \"to\": \"TapNumber\"\n" +
                "    },\n" +
                "    \"myint[($bit)][unsigned]\":{\"bit\":48, \"bitRatio\": 2, \"unsigned\":\"unsigned\", \"to\":\"TapNumber\"},\n" +
                "    \"real\": {\n" +
                "      \"bit\": 32,\n" +
                "      \"priority\": 2,\n" +
                "      \"precision\": [\n" +
                "        1,\n" +
                "        6\n" +
                "      ],\n" +
                "      \"scale\": [\n" +
                "        0,\n" +
                "        6\n" +
                "      ],\n" +
                "      \"fixed\": false,\n" +
                "      \"to\": \"TapNumber\"\n" +
                "    }" +
                "}";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("tapNumber(20, -3)", "tapNumber(20, -3)"))
        ;

        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();


        TapField number20m3 = nameFieldMap.get("tapNumber(20, -3)");
        assertEquals("numeric(20,-3)", number20m3.getDataType());
    }


    @Test
    public void customYearCodecTest() {

        String sourceTypeExpression = "{" +
                "\"YEAR\": {\"to\": \"TapYear\"}" +
                "}";

        String targetTypeExpression = "{\n" +
//                "\"int unsigned\": {\"to\": \"TapNumber\",\"byte\": 32,\"value\": [\"0\", \"4294967295\"]}," +
                "\"CLOB\": {\"byte\": \"4g\",\"pkEnablement\": false,\"priority\": 2,\"to\": \"TapString\"}," +
                "\"VARCHAR2[($byte)]\": {\"byte\": 4000,\"priority\": 1,\"preferByte\": 2000,\"to\": \"TapString\"}" +
                "}";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("YEAR", "YEAR"));

        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));

        TapCodecsRegistry targetRegistry = TapCodecsRegistry.create();
        targetRegistry.registerFromTapValue(TapYearValue.class, "DATE", tapValue -> tapValue.getValue() + "_YYYY");
        TapCodecsFilterManager targetCodecFilterManager = TapCodecsFilterManager.create(targetRegistry);

        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();


        TapField upperVarchar50 = nameFieldMap.get("YEAR");
        assertEquals("DATE", upperVarchar50.getDataType());
    }

    @Test
    public void tddBinaryToTDEngineTest() {
        String targetTypeExpression = "{\n" +
//                "\"int unsigned\": {\"to\": \"TapNumber\",\"byte\": 32,\"value\": [\"0\", \"4294967295\"]}," +
                "\"binary($byte)\": {\n" +
                "      \"to\": \"TapBinary\",\n" +
                "      \"byte\": 255,\n" +
                "      \"defaultByte\": 1,\n" +
                "      \"preferByte\": 2000,\n" +
                "      \"fixed\": true\n" +
                "    }" +
                "}";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("binary", "binary").tapType(tapBinary().bytes(100L)));

        TapCodecsRegistry targetRegistry = TapCodecsRegistry.create();
        TapCodecsFilterManager targetCodecFilterManager = TapCodecsFilterManager.create(targetRegistry);

        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();

        TapField upperVarchar50 = nameFieldMap.get("binary");
        assertEquals("binary(100)", upperVarchar50.getDataType());
    }

    @Test
    public void oracleChar10ToESTest() {

        String sourceTypeExpression = "{" +
                "\"CHAR[($byte)]\": {\n" +
                "        \"byte\": 2000,\n" +
                "                \"priority\": 1,\n" +
                "                \"defaultByte\": 1,\n" +
                "                \"fixed\": true,\n" +
                "                \"to\": \"TapString\"\n" +
                "    }" +
                "}";

        String targetTypeExpression = "{\n" +
                    "\"string\": {\n" +
                "      \"queryOnly\": true,\n" +
                "      \"to\": \"TapString\"\n" +
                "    },\n" +
                "    \"text\": {\n" +
                "      \"byte\": \"4g\",\n" +
                "      \"to\": \"TapString\"\n" +
                "    },\n" +
                "    \"keyword\": {\n" +
                "      \"byte\": 32766,\n" +
                "      \"to\": \"TapString\"\n" +
                "    },\n" +
                "\"object\": {\n" +
                "      \"to\": \"TapString\"\n" +
                "    }" +
                "}";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("char10", "char(10)"));

        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));

        TapCodecsRegistry targetRegistry = TapCodecsRegistry.create();
        TapCodecsFilterManager targetCodecFilterManager = TapCodecsFilterManager.create(targetRegistry);

        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();


        TapField upperVarchar50 = nameFieldMap.get("char10");
        assertEquals("keyword", upperVarchar50.getDataType());
    }
    @Test
    public void mysqlVarcharToDorisCharTest() {
        String sourceTypeExpression = "{" +
                "\"char[($byte)]\": {\n" +
                "      \"to\": \"TapString\",\n" +
                "      \"byte\": 255,\n" +
                "      \"defaultByte\": 1,\n" +
                "      \"byteRatio\": 3,\n" +
                "      \"fixed\": true\n" +
                "    },\n" +
                "    \"varchar($byte)\": {\n" +
                "      \"name\": \"varchar\",\n" +
                "      \"to\": \"TapString\",\n" +
                "      \"byte\": 16358,\n" +
                "      \"defaultByte\": 1,\n" +
                "      \"byteRatio\": 3\n" +
                "    }" +
                "}";

        String targetTypeExpression = "{\n" +
                " \"char[($byte)]\": {\n" +
                "      \"byte\": 255,\n" +
                "      \"to\": \"TapString\",\n" +
                "      \"defaultByte\": 1\n" +
                "    },\n" +
                "    \"varchar[($byte)]\": {\n" +
                "      \"byte\": \"65535\",\n" +
                "      \"to\": \"TapString\",\n" +
                "      \"defaultByte\": 1\n" +
                "    }" +
                "}";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("char10", "char(10)"))
                .add(field("char100", "char(100)"))
                .add(field("varchar10", "varchar(10)"))
                .add(field("varchar100", "varchar(100)"));

        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));

        TapCodecsRegistry targetRegistry = TapCodecsRegistry.create();
        TapCodecsFilterManager targetCodecFilterManager = TapCodecsFilterManager.create(targetRegistry);

        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();


        TapField char10 = nameFieldMap.get("char10");
        assertEquals("char(30)", char10.getDataType());
        TapField char100 = nameFieldMap.get("char100");
        assertEquals("varchar(300)", char100.getDataType());
        TapField varchar10 = nameFieldMap.get("varchar10");
        assertEquals("char(30)", varchar10.getDataType());
        TapField varchar100 = nameFieldMap.get("varchar100");
        assertEquals("varchar(300)", varchar100.getDataType());
    }

    @Test
    public void mysqlToKafkaTest() {
        String sourceTypeExpression = "{\n" +
                "    \"char[($byte)]\": {\n" +
                "      \"to\": \"TapString\",\n" +
                "      \"byte\": 255,\n" +
                "      \"defaultByte\": 1,\n" +
                "      \"byteRatio\": 3,\n" +
                "      \"fixed\": true\n" +
                "    },\n" +
                "    \"varchar($byte)\": {\n" +
                "      \"name\": \"varchar\",\n" +
                "      \"to\": \"TapString\",\n" +
                "      \"byte\": 16358,\n" +
                "      \"defaultByte\": 1,\n" +
                "      \"byteRatio\": 3\n" +
                "    },\n" +
                "    \"tinytext\": {\n" +
                "      \"to\": \"TapString\",\n" +
                "      \"byte\": 255,\n" +
                "      \"pkEnablement\": false\n" +
                "    },\n" +
                "    \"text\": {\n" +
                "      \"to\": \"TapString\",\n" +
                "      \"byte\": \"64k\",\n" +
                "      \"pkEnablement\": false\n" +
                "    },\n" +
                "    \"mediumtext\": {\n" +
                "      \"to\": \"TapString\",\n" +
                "      \"byte\": \"16m\",\n" +
                "      \"pkEnablement\": false\n" +
                "    },\n" +
                "    \"longtext\": {\n" +
                "      \"to\": \"TapString\",\n" +
                "      \"byte\": \"4g\",\n" +
                "      \"pkEnablement\": false\n" +
                "    },\n" +
                "    \"json\": {\n" +
                "      \"to\": \"TapMap\",\n" +
                "      \"byte\": \"4g\",\n" +
                "      \"pkEnablement\": false\n" +
                "    },\n" +
                "    \"binary[($byte)]\": {\n" +
                "      \"to\": \"TapBinary\",\n" +
                "      \"byte\": 255,\n" +
                "      \"defaultByte\": 1,\n" +
                "      \"fixed\": true\n" +
                "    },\n" +
                "    \"varbinary[($byte)]\": {\n" +
                "      \"to\": \"TapBinary\",\n" +
                "      \"byte\": 65532,\n" +
                "      \"defaultByte\": 1\n" +
                "    },\n" +
                "    \"tinyblob\": {\n" +
                "      \"to\": \"TapBinary\",\n" +
                "      \"byte\": 255\n" +
                "    },\n" +
                "    \"blob\": {\n" +
                "      \"to\": \"TapBinary\",\n" +
                "      \"byte\": \"64k\"\n" +
                "    },\n" +
                "    \"mediumblob\": {\n" +
                "      \"to\": \"TapBinary\",\n" +
                "      \"byte\": \"16m\"\n" +
                "    },\n" +
                "    \"longblob\": {\n" +
                "      \"to\": \"TapBinary\",\n" +
                "      \"byte\": \"4g\"\n" +
                "    },\n" +
                "    \"bit[($bit)]\": {\n" +
                "      \"to\": \"TapNumber\",\n" +
                "      \"bit\": 64,\n" +
                "      \"queryOnly\": true\n" +
                "    },\n" +
                "    \"tinyint[($zerofill)]\": {\n" +
                "      \"to\": \"TapNumber\",\n" +
                "      \"bit\": 8,\n" +
                "      \"precision\": 3,\n" +
                "      \"value\": [\n" +
                "        -128,\n" +
                "        127\n" +
                "      ]\n" +
                "    },\n" +
                "    \"tinyint[($zerofill)] unsigned\": {\n" +
                "      \"to\": \"TapNumber\",\n" +
                "      \"bit\": 8,\n" +
                "      \"precision\": 3,\n" +
                "      \"value\": [\n" +
                "        0,\n" +
                "        255\n" +
                "      ],\n" +
                "      \"unsigned\": \"unsigned\"\n" +
                "    },\n" +
                "    \"smallint[($zerofill)]\": {\n" +
                "      \"to\": \"TapNumber\",\n" +
                "      \"bit\": 16,\n" +
                "      \"value\": [\n" +
                "        -32768,\n" +
                "        32767\n" +
                "      ],\n" +
                "      \"precision\": 5\n" +
                "    },\n" +
                "    \"smallint[($zerofill)] unsigned\": {\n" +
                "      \"to\": \"TapNumber\",\n" +
                "      \"bit\": 16,\n" +
                "      \"precision\": 5,\n" +
                "      \"value\": [\n" +
                "        0,\n" +
                "        65535\n" +
                "      ],\n" +
                "      \"unsigned\": \"unsigned\"\n" +
                "    },\n" +
                "    \"mediumint[($zerofill)]\": {\n" +
                "      \"to\": \"TapNumber\",\n" +
                "      \"bit\": 24,\n" +
                "      \"precision\": 7,\n" +
                "      \"value\": [\n" +
                "        -8388608,\n" +
                "        8388607\n" +
                "      ]\n" +
                "    },\n" +
                "    \"mediumint[($zerofill)] unsigned\": {\n" +
                "      \"to\": \"TapNumber\",\n" +
                "      \"bit\": 24,\n" +
                "      \"precision\": 8,\n" +
                "      \"value\": [\n" +
                "        0,\n" +
                "        16777215\n" +
                "      ],\n" +
                "      \"unsigned\": \"unsigned\"\n" +
                "    },\n" +
                "    \"int[($zerofill)]\": {\n" +
                "      \"to\": \"TapNumber\",\n" +
                "      \"bit\": 32,\n" +
                "      \"precision\": 10,\n" +
                "      \"value\": [\n" +
                "        -2147483648,\n" +
                "        2147483647\n" +
                "      ]\n" +
                "    },\n" +
                "    \"int[($zerofill)] unsigned\": {\n" +
                "      \"to\": \"TapNumber\",\n" +
                "      \"bit\": 32,\n" +
                "      \"precision\": 10,\n" +
                "      \"value\": [\n" +
                "        0,\n" +
                "        4294967295\n" +
                "      ]\n" +
                "    },\n" +
                "    \"bigint[($zerofill)]\": {\n" +
                "      \"to\": \"TapNumber\",\n" +
                "      \"bit\": 64,\n" +
                "      \"precision\": 19,\n" +
                "      \"value\": [\n" +
                "        -9223372036854775808,\n" +
                "        9223372036854775807\n" +
                "      ]\n" +
                "    },\n" +
                "    \"bigint[($zerofill)] unsigned\": {\n" +
                "      \"to\": \"TapNumber\",\n" +
                "      \"bit\": 64,\n" +
                "      \"precision\": 20,\n" +
                "      \"value\": [\n" +
                "        0,\n" +
                "        18446744073709551615\n" +
                "      ]\n" +
                "    },\n" +
                "    \"decimal[($precision,$scale)][unsigned]\": {\n" +
                "      \"to\": \"TapNumber\",\n" +
                "      \"precision\": [\n" +
                "        1,\n" +
                "        65\n" +
                "      ],\n" +
                "      \"scale\": [\n" +
                "        0,\n" +
                "        30\n" +
                "      ],\n" +
                "      \"defaultPrecision\": 10,\n" +
                "      \"defaultScale\": 0,\n" +
                "      \"unsigned\": \"unsigned\",\n" +
                "      \"fixed\": true\n" +
                "    },\n" +
                "    \"float($precision,$scale)[unsigned]\": {\n" +
                "      \"to\": \"TapNumber\",\n" +
                "      \"name\": \"float\",\n" +
                "      \"precision\": [\n" +
                "        1,\n" +
                "        30\n" +
                "      ],\n" +
                "      \"scale\": [\n" +
                "        0,\n" +
                "        30\n" +
                "      ],\n" +
                "      \"value\": [\n" +
                "        \"-3.402823466E+38\",\n" +
                "        \"3.402823466E+38\"\n" +
                "      ],\n" +
                "      \"unsigned\": \"unsigned\",\n" +
                "      \"fixed\": false\n" +
                "    },\n" +
                "    \"float\": {\n" +
                "      \"to\": \"TapNumber\",\n" +
                "      \"precision\": [\n" +
                "        1,\n" +
                "        6\n" +
                "      ],\n" +
                "      \"scale\": [\n" +
                "        0,\n" +
                "        6\n" +
                "      ],\n" +
                "      \"fixed\": false\n" +
                "    },\n" +
                "    \"double\": {\n" +
                "      \"to\": \"TapNumber\",\n" +
                "      \"precision\": [\n" +
                "        1,\n" +
                "        17\n" +
                "      ],\n" +
                "      \"preferPrecision\": 11,\n" +
                "      \"preferScale\": 4,\n" +
                "      \"scale\": [\n" +
                "        0,\n" +
                "        17\n" +
                "      ],\n" +
                "      \"fixed\": false\n" +
                "    },\n" +
                "    \"double[($precision,$scale)][unsigned]\": {\n" +
                "      \"to\": \"TapNumber\",\n" +
                "      \"precision\": [\n" +
                "        1,\n" +
                "        255\n" +
                "      ],\n" +
                "      \"scale\": [\n" +
                "        0,\n" +
                "        30\n" +
                "      ],\n" +
                "      \"value\": [\n" +
                "        \"-1.7976931348623157E+308\",\n" +
                "        \"1.7976931348623157E+308\"\n" +
                "      ],\n" +
                "      \"unsigned\": \"unsigned\",\n" +
                "      \"fixed\": false\n" +
                "    },\n" +
                "    \"date\": {\n" +
                "      \"to\": \"TapDate\",\n" +
                "      \"range\": [\n" +
                "        \"1000-01-01\",\n" +
                "        \"9999-12-31\"\n" +
                "      ],\n" +
                "      \"pattern\": \"yyyy-MM-dd\"\n" +
                "    },\n" +
                "    \"time[($fraction)]\": {\n" +
                "      \"to\": \"TapTime\",\n" +
                "      \"fraction\": [\n" +
                "        0,\n" +
                "        6\n" +
                "      ],\n" +
                "      \"defaultFraction\": 0,\n" +
                "      \"range\": [\n" +
                "        \"-838:59:59\",\n" +
                "        \"838:59:59\"\n" +
                "      ],\n" +
                "      \"pattern\": \"HH:mm:ss\"\n" +
                "    },\n" +
                "    \"datetime[($fraction)]\": {\n" +
                "      \"to\": \"TapDateTime\",\n" +
                "      \"range\": [\n" +
                "        \"1000-01-01 00:00:00\",\n" +
                "        \"9999-12-31 23:59:59\"\n" +
                "      ],\n" +
                "      \"pattern\": \"yyyy-MM-dd HH:mm:ss\",\n" +
                "      \"fraction\": [\n" +
                "        0,\n" +
                "        6\n" +
                "      ],\n" +
                "      \"defaultFraction\": 0\n" +
                "    },\n" +
                "    \"timestamp[($fraction)]\": {\n" +
                "      \"to\": \"TapDateTime\",\n" +
                "      \"range\": [\n" +
                "        \"1970-01-01 00:00:01\",\n" +
                "        \"2038-01-19 03:14:07\"\n" +
                "      ],\n" +
                "      \"pattern\": \"yyyy-MM-dd HH:mm:ss\",\n" +
                "      \"fraction\": [\n" +
                "        0,\n" +
                "        6\n" +
                "      ],\n" +
                "      \"defaultFraction\": 0,\n" +
                "      \"withTimeZone\": true\n" +
                "    },\n" +
                "    \"year[($fraction)]\": {\n" +
                "      \"to\": \"TapYear\",\n" +
                "      \"range\": [\n" +
                "        \"1901\",\n" +
                "        \"2155\"\n" +
                "      ],\n" +
                "      \"fraction\": [\n" +
                "        0,\n" +
                "        4\n" +
                "      ],\n" +
                "      \"defaultFraction\": 4,\n" +
                "      \"pattern\": \"yyyy\"\n" +
                "    },\n" +
                "    \"enum($enums)\": {\n" +
                "      \"name\": \"enum\",\n" +
                "      \"to\": \"TapString\",\n" +
                "      \"queryOnly\": true,\n" +
                "      \"byte\": 16383\n" +
                "    },\n" +
                "    \"set($sets)\": {\n" +
                "      \"name\": \"set\",\n" +
                "      \"to\": \"TapString\",\n" +
                "      \"queryOnly\": true,\n" +
                "      \"byte\": 16383\n" +
                "    },\n" +
                "    \"point\": {\n" +
                "      \"to\": \"TapBinary\",\n" +
                "      \"queryOnly\": true\n" +
                "    },\n" +
                "    \"linestring\": {\n" +
                "      \"to\": \"TapBinary\",\n" +
                "      \"queryOnly\": true\n" +
                "    },\n" +
                "    \"polygon\": {\n" +
                "      \"to\": \"TapBinary\",\n" +
                "      \"queryOnly\": true\n" +
                "    },\n" +
                "    \"geometry\": {\n" +
                "      \"to\": \"TapBinary\",\n" +
                "      \"queryOnly\": true\n" +
                "    },\n" +
                "    \"multipoint\": {\n" +
                "      \"to\": \"TapBinary\",\n" +
                "      \"queryOnly\": true\n" +
                "    },\n" +
                "    \"multilinestring\": {\n" +
                "      \"to\": \"TapBinary\",\n" +
                "      \"queryOnly\": true\n" +
                "    },\n" +
                "    \"multipolygon\": {\n" +
                "      \"to\": \"TapBinary\",\n" +
                "      \"queryOnly\": true\n" +
                "    },\n" +
                "    \"geomcollection\": {\n" +
                "      \"to\": \"TapBinary\",\n" +
                "      \"queryOnly\": true\n" +
                "    }\n" +
                "  }";

        String targetTypeExpression = "{\n" +
                "        \"OBJECT\": {\n" +
                "        \"to\": \"TapMap\"\n" +
                "    },\n" +
                "        \"ARRAY\": {\n" +
                "        \"to\": \"TapArray\"\n" +
                "    },\n" +
                "        \"NUMBER\": {\n" +
                "        \"precision\": [\n" +
                "        1,\n" +
                "                1000\n" +
                "      ],\n" +
                "        \"scale\": [\n" +
                "        0,\n" +
                "                1000\n" +
                "      ],\n" +
                "        \"fixed\": true,\n" +
                "                \"preferPrecision\": 20,\n" +
                "                \"preferScale\": 8,\n" +
                "                \"priority\": 1,\n" +
                "                \"to\": \"TapNumber\"\n" +
                "    },\n" +
                "        \"INTEGER\": {\n" +
                "        \"bit\": 32,\n" +
                "                \"priority\": 1,\n" +
                "                \"value\": [\n" +
                "        -2147483648,\n" +
                "                2147483647\n" +
                "      ],\n" +
                "        \"to\": \"TapNumber\"\n" +
                "    },\n" +
                "        \"BOOLEAN\": {\n" +
                "        \"to\": \"TapBoolean\"\n" +
                "    },\n" +
                "        \"STRING\": {\n" +
                "        \"byte\": 200,\n" +
                "                \"priority\": 1,\n" +
                "                \"defaultByte\": 200,\n" +
                "                \"preferByte\": 200,\n" +
                "                \"to\": \"TapString\"\n" +
                "    },\n" +
                "        \"TEXT\": {\n" +
                "        \"to\": \"TapString\"\n" +
                "    }\n" +
                "    }";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("char10", "char(10)"))
                .add(field("date", "date"))
                .add(field("datetime", "datetime"))
                .add(field("datetime(6)", "datetime(6)"));

        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));

        TapCodecsRegistry targetRegistry = TapCodecsRegistry.create();
        TapCodecsFilterManager targetCodecFilterManager = TapCodecsFilterManager.create(targetRegistry);

        Map<String, PossibleDataTypes> findPossibleDataTypes = new LinkedHashMap<>();
        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager, findPossibleDataTypes);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();

        assertEquals(4, findPossibleDataTypes.size());

    }
}

package com.tapdata.tm.config;

import com.tapdata.tm.config.convert.BigIntegerReadConverter;
import com.tapdata.tm.config.convert.BigIntegerWriteConverter;
import com.tapdata.tm.commons.schema.Field;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.convert.UpdateMapper;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.dao.support.PersistenceExceptionTranslator;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DefaultMongoConfigTest {

    @Test
    void testMongoCustomConversionsShouldRegisterBigIntegerWriteConverter() {
        DefaultMongoConfig defaultMongoConfig = new DefaultMongoConfig();

        MongoCustomConversions conversions = defaultMongoConfig.mongoCustomConversions();

        assertTrue(conversions.hasCustomWriteTarget(BigInteger.class, String.class));
    }

    @Test
    void testBigIntegerWriteConverterShouldPreserveFullValueAsString() {
        BigIntegerWriteConverter converter = new BigIntegerWriteConverter();

        String actual = converter.convert(new BigInteger("123456789012345678901234567890"));

        assertEquals("123456789012345678901234567890", actual);
    }

    @Test
    void testMongoCustomConversionsShouldRegisterBigIntegerReadConverter() {
        DefaultMongoConfig defaultMongoConfig = new DefaultMongoConfig();

        MongoCustomConversions conversions = defaultMongoConfig.mongoCustomConversions();

        assertTrue(conversions.hasCustomReadTarget(String.class, BigInteger.class));
    }

    @Test
    void testBigIntegerReadConverterShouldParseFullValueFromString() {
        BigIntegerReadConverter converter = new BigIntegerReadConverter();

        BigInteger actual = converter.convert("123456789012345678901234567890");

        assertEquals(new BigInteger("123456789012345678901234567890"), actual);
    }

    @Test
    void testUpdateMapperShouldWriteBigIntegerFieldDefaultsAsString() {
        MongoCustomConversions conversions = new DefaultMongoConfig().mongoCustomConversions();
        MongoMappingContext mappingContext = new MongoMappingContext();
        mappingContext.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
        mappingContext.afterPropertiesSet();
        MongoDatabaseFactory databaseFactory = mock(MongoDatabaseFactory.class);
        when(databaseFactory.getExceptionTranslator()).thenReturn(mock(PersistenceExceptionTranslator.class));
        MappingMongoConverter converter = new MappingMongoConverter(
                new DefaultDbRefResolver(databaseFactory), mappingContext);
        converter.setCustomConversions(conversions);
        converter.afterPropertiesSet();

        BigInteger bigInteger = new BigInteger("123456789012345678901234567890");
        Field field = new Field();
        field.setDefaultValue(bigInteger);
        field.setOriginalDefaultValue(bigInteger);
        Update update = new Update().set("fields", Collections.singletonList(field));

        Document mappedUpdate = new UpdateMapper(converter).getMappedObject(update.getUpdateObject(), (MongoPersistentEntity<?>) null);
        Document mappedField = (Document) ((List<?>) ((Document) mappedUpdate.get("$set")).get("fields")).get(0);

        assertEquals("123456789012345678901234567890", mappedField.get("default_value"));
        assertEquals("123456789012345678901234567890", mappedField.get("originalDefaultValue"));
    }
}

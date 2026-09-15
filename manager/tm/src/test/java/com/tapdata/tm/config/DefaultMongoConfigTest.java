package com.tapdata.tm.config;

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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression tests for the Spring Boot 4 / Spring Data MongoDB 5.x migration, which changed the
 * default {@link MongoCustomConversions.BigDecimalRepresentation} from STRING to UNSPECIFIED.
 * Without an explicit STRING configuration, BigInteger/BigDecimal field default values (e.g.
 * derived from unsigned BIGINT columns) fail to encode or silently become Decimal128. The STRING
 * representation — and the symmetric framework-provided read/write converters behind it — is what
 * the persistence format relies on, so these tests pin it down.
 */
class DefaultMongoConfigTest {

    @Test
    void testMongoCustomConversionsShouldPersistBigIntegerAndBigDecimalAsString() {
        MongoCustomConversions conversions = new DefaultMongoConfig().mongoCustomConversions();

        assertTrue(conversions.hasCustomWriteTarget(BigInteger.class, String.class));
        assertTrue(conversions.hasCustomWriteTarget(BigDecimal.class, String.class));
    }

    @Test
    void testUpdateMapperShouldWriteBigIntegerFieldDefaultsAsString() {
        MappingMongoConverter converter = createProductionMappingMongoConverter();

        BigInteger bigInteger = new BigInteger("18446744073709551615");
        Field field = new Field();
        field.setDefaultValue(bigInteger);
        field.setOriginalDefaultValue(bigInteger);
        Update update = new Update().set("fields", Collections.singletonList(field));

        Document mappedUpdate = new UpdateMapper(converter).getMappedObject(update.getUpdateObject(), (MongoPersistentEntity<?>) null);
        Document mappedField = (Document) ((List<?>) ((Document) mappedUpdate.get("$set")).get("fields")).get(0);

        assertEquals("18446744073709551615", mappedField.get("default_value"));
        assertEquals("18446744073709551615", mappedField.get("originalDefaultValue"));
    }

    @Test
    void testUpdateMapperShouldWriteBigDecimalFieldDefaultAsStringNotDecimal128() {
        MappingMongoConverter converter = createProductionMappingMongoConverter();

        BigDecimal bigDecimal = new BigDecimal("123456789012345678901234567890.123456789");
        Field field = new Field();
        field.setDefaultValue(bigDecimal);

        Update update = new Update().set("fields", Collections.singletonList(field));

        Document mappedUpdate = new UpdateMapper(converter).getMappedObject(update.getUpdateObject(), (MongoPersistentEntity<?>) null);
        Document mappedField = (Document) ((List<?>) ((Document) mappedUpdate.get("$set")).get("fields")).get(0);

        assertEquals(bigDecimal.toString(), mappedField.get("default_value"));
        assertTrue(mappedField.get("default_value") instanceof String);
    }

    @Test
    void testUpdateMapperShouldWriteBigDecimalAsStringInNestedPushEach() {
        MappingMongoConverter converter = createProductionMappingMongoConverter();
        BigDecimal bigDecimal = new BigDecimal("123456789012345678901234567890.123456789");
        Field field = new Field();
        field.setDefaultValue(bigDecimal);

        Update update = new Update().push("histories").each(Collections.singletonList(
                Collections.singletonMap("fields", Collections.singletonList(field))));

        Document mappedUpdate = new UpdateMapper(converter)
                .getMappedObject(update.getUpdateObject(), (MongoPersistentEntity<?>) null);
        Document push = (Document) mappedUpdate.get("$push");
        Document histories = (Document) push.get("histories");
        List<?> each = (List<?>) histories.get("$each");
        Document history = (Document) each.get(0);
        Document mappedField = (Document) ((List<?>) history.get("fields")).get(0);

        assertEquals(bigDecimal.toString(), mappedField.get("default_value"));
    }

    private MappingMongoConverter createProductionMappingMongoConverter() {
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
        return converter;
    }
}

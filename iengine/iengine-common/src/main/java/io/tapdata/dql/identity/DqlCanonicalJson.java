package io.tapdata.dql.identity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.tapdata.constant.JSONUtil;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * Produces the canonical JSON hashes used by DLQ identities.
 */
final class DqlCanonicalJson {
    private final ObjectMapper objectMapper;

    DqlCanonicalJson() {
        this(JSONUtil.mapper);
    }

    DqlCanonicalJson(ObjectMapper objectMapper) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null").copy()
                .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
                .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    }

    String sha256(Object value) {
        try {
            byte[] canonicalBytes = objectMapper.writeValueAsBytes(value);
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(canonicalBytes);
            StringBuilder result = new StringBuilder("sha256:");
            for (byte valueByte : digest) {
                result.append(Character.forDigit((valueByte >>> 4) & 0x0f, 16));
                result.append(Character.forDigit(valueByte & 0x0f, 16));
            }
            return result.toString();
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("DLQ identity value cannot be serialized", exception);
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is not available", exception);
        }
    }
}

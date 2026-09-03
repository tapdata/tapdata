package com.tapdata.tm.modules.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.module.entity.Path;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PublishApiTest {

    @Test
    void simplifiedPathKeepsFullCustomQueryFlagAlongsideCustomWhere() {
        String customWhere = "{\"$and\":[]}";
        Path path = new Path();
        path.setName("customerQuery");
        path.setFullCustomQuery(true);
        path.setCustomWhere(customWhere);

        ModulesDto module = new ModulesDto();
        module.setPaths(List.of(path));

        JsonNode simplifiedPath = new ObjectMapper()
                .valueToTree(PublishApi.from(module))
                .get("paths")
                .get(0);

        assertNotNull(simplifiedPath.get("fullCustomQuery"));
        assertNotNull(simplifiedPath.get("customWhere"));
        assertTrue(simplifiedPath.get("fullCustomQuery").isBoolean());
        assertTrue(simplifiedPath.get("fullCustomQuery").asBoolean());
        assertEquals(customWhere, simplifiedPath.get("customWhere").asText());
    }
}

package com.tapdata.tm.utils;

import com.tapdata.tm.base.dto.Where;
import org.bson.Document;
import org.springframework.data.mongodb.core.query.Criteria;

public class QueryUtil {
    private QueryUtil() {}
    /**
     * Convert filter to Criteria
     *
     * @param where
     * @return
     */
    public static Criteria parseWhereToCriteria(Where where) {
        Document doc = new Document(where);
        return Criteria.matchingDocumentStructure(() -> doc);
    }
}

package com.tapdata.tm.utils;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Where;
import org.bson.Document;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.schema.MongoJsonSchema;

import java.util.ArrayList;
import java.util.List;

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
        return Criteria.matchingDocumentStructure(new MongoJsonSchema() {
            @Override
            public Document schemaDocument() {
                return doc;
            }
            @Override
            public Document toDocument() {
                return doc;
            }

        });
    }

    public static void parsePageParam(Filter filter, Query query) {
        int limit = filter.getLimit();
        int skip = filter.getSkip();
        query.skip(skip).limit(limit);
    }

    public static List<Sort> parseOrder(Filter filter) {
        List<String> sort = filter.getSort();
        List<Sort> sortList = new ArrayList<>();
        for (String s : sort) {
            String[] split = s.split(" ");
            if (split.length == 2) {
                String type = String.valueOf(split[1]).toUpperCase();
                switch (type) {
                    case "DESC": sortList.add(Sort.by(Sort.Order.desc(split[0])));break;
                    case "ASC": sortList.add(Sort.by(Sort.Order.asc(split[0])));break;
                    default: sortList.add(Sort.by(Sort.Order.by(split[0])));
                }
            }
        }
        return sortList;
    }

    public static Sort parseOrder(String order) {
        if (null != order && !order.trim().isEmpty()) {
            String[] splits = order.trim().split(" +");
            Sort sort = Sort.by(splits[0].trim());
            if (splits.length == 1) {
                return sort.descending();
            }

            if (Sort.Direction.ASC.name().equalsIgnoreCase(splits[1].trim())) {
                return sort.ascending();
            }
            return sort.descending();
        }
        return null;
    }
}

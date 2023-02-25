package com.tapdata.tm.utils;

import com.tapdata.manager.common.utils.DateUtil;
import com.tapdata.tm.base.dto.Field;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.support.MappingMongoEntityInformation;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.tapdata.manager.common.utils.ReflectionUtils.getAllFieldType;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/8/4 7:58 上午
 * @description
 */
public class MongoUtils {

    private static final Logger logger = LoggerFactory.getLogger(MongoUtils.class);

    private static final String[] REGEX_CHAR = {"\\", "^", "*", "+", "?", "(", ")", "{", "}", "[", "]", ".", "|", "/"};

    /**
     * Compile query conditions according to the request
     *
     * @param where
     * @return
     */
    public static <Entity, ID> Criteria buildCriteria(Map<String, Object> where, MappingMongoEntityInformation<Entity, ID> entityInformation) {
        if (where != null) {

            preConvertWhereOptions(where, entityInformation, getAllFieldType(entityInformation.getJavaType()));
            Document doc = new Document(where);

            return Criteria.matchingDocumentStructure(() -> doc);
        }

        return new Criteria();
    }

    private static Date convertToDate(Object value) {
        if (value instanceof Double) {
            return new Date(((Double) value).longValue());
        } else if (value instanceof Integer) {
            return new Date(Long.valueOf((Integer) value));
        } else if (value instanceof Long) {
            return new Date((Long) value);
        } else if (value instanceof String) {
            try {
                return DateUtil.parse((String) value);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private static <Entity, ID> void preConvertWhereOptions(Map<String, Object> where,
                                                            MappingMongoEntityInformation<Entity, ID> entityInformation,
                                                            Map<String, Class<?>> fieldTypes) {
        convertSymbol(where);
        transformTimeRange(where);


        for (Map.Entry<String, Object> mapEntry : where.entrySet()) {
            String key = mapEntry.getKey();
            Object cond = mapEntry.getValue();
            if (fieldTypes.containsKey(key) && fieldTypes.get(key) == Date.class) {
                if (cond instanceof Map) {
                    Map map = (Map) cond;
                    if (map.size() == 1) {
                        map.keySet().forEach(key1 -> {
                            Object value1 = map.get(key1);
                            Date date = convertToDate(value1);
                            if (date != null) {
                                map.replace(key, date);
                            }
                        });
                    }
                } else {
                    Date date = convertToDate(cond);
                    if (date != null) {
                        where.replace(key, date);
                    }
                }
            } else if (fieldTypes.containsKey(key) && fieldTypes.get(key) == ObjectId.class) {
                if (cond instanceof String) {
                    ObjectId objectId = toObjectId((String) cond);
                    if (objectId != null) {
                        where.replace(key, objectId);
                        continue;
                    }
                } else if (cond instanceof Map) {
                    Map condForMap = (Map) cond;
                    condForMap.keySet().forEach(spec -> {
                        Object val = condForMap.get(spec);
                        if (val instanceof List) {
                            condForMap.put(spec, ((List) val).stream().map(v -> toObjectId(v != null ? v.toString() : null)).collect(Collectors.toList()));
                        }
                    });
                }
            }

            if ((key.equals("id") || key.equals("_id") || key.equals("listtags.id")) && cond instanceof String) {
                if (((String) cond).matches("[0-9a-zA-Z]{24}")) {
                    where.replace(key, toObjectId((String) cond));
                }
            }

            if (cond instanceof Map) {
                if (((Map) cond).containsKey("$oid")) {
                    Object str = ((Map) cond).get("$oid");
                    if (str != null) {
                        ObjectId objectId = toObjectId(str.toString());
                        if (objectId != null) {
                            where.replace(key, objectId);
                        }
                    }
                } else if (((Map) cond).containsKey("like")) {
                    HashMap<String, Object> map = new HashMap<>();
                    String like = ((Map) cond).get("like").toString();
                    for (String s : REGEX_CHAR) {
                        if (like.contains(s)) {
                            like = like.replace(s, "\\" + s);
                        }
                    }
                    map.put("$regex", like);
                    if (((Map) cond).containsKey("options")) {
                        map.put("$options", ((Map) cond).get("options"));
                    }
                    where.put(key, map);
                } else if (((Map) cond).containsKey("$inq")) {
                    ((Map) cond).put("$in", ((Map) cond).remove("$inq"));
                } else if (((Map) cond).containsKey("inq")) {
                    ((Map) cond).put("$in", ((Map) cond).remove("inq"));
                } else if ((key.equals("id") || key.equals("_id"))) {
                    //对于那种对id进行复合操作的做一下转化
                    Map<String, Object> value1 = (Map<String, Object>) cond;
                    convertSymbol(value1);
                    for (Map.Entry<String, Object> entry : value1.entrySet()) {
                        String k = entry.getKey();
                        Object v = entry.getValue();
                        if (v instanceof String) {
                            if (((String) v).matches("[0-9a-zA-Z]{24}")) {
                                value1.put(k, toObjectId((String) v));
                            }
                        } else if (v instanceof List) {
                            List v1 = (List) v;
                            boolean replace = false;
                            List v2 = new ArrayList();
                            for (Object item : v1) {
                                if (item instanceof String) {
                                    if (((String) item).matches("[0-9a-zA-Z]{24}")) {
                                        v2.add(toObjectId((String) item));
                                        replace = true;
                                    } else {
                                        v2.add(item);
                                    }
                                }

                            }
                            if (replace) {
                                value1.put(k, v2);
                            }
                        }
                    }

                } else {
                    preConvertWhereOptions((Map) cond, entityInformation, fieldTypes);
                }
            } else if (cond instanceof List) {
                ((List) cond).forEach(item -> {
                    if (item instanceof Map) {
                        preConvertWhereOptions((Map) item, entityInformation, fieldTypes);
                    }
                });
            } else if (cond instanceof String) {
                try {
                    Date date = DateUtil.parse((String) cond);
                    if (date != null) {
                        where.replace(key, date);
                    }
                } catch (Exception e) {
                    //do nothing
                    //e.printStackTrace();
                }
            } else if (cond instanceof ObjectId) {

            } else if (cond != null) {
                //logger.warn("Unhandled document type " + cond.getClass().getTypeName());
            }
        }
    }

    private static void convertSymbol(Map<String, Object> map){
        if (map.containsKey("or")) {
            map.put("$or", map.remove("or"));
        }
        if (map.containsKey("and")) {
            map.put("$and", map.remove("and"));
        }
        if (map.containsKey("eq")) {
            map.put("$eq", map.remove("eq"));
        }
        if (map.containsKey("lt")) {
            map.put("$lt", map.remove("lt"));
        }
        if (map.containsKey("gt")) {
            map.put("$gt", map.remove("gt"));
        }
    }


    /**
     * set query sorting according to request
     *
     * @param query
     * @param sorts
     */
    public static void applySort(Query query, List<String> sorts) {

        if (query == null || sorts == null)
            return;

        sorts.forEach(field -> {
            if (!StringUtils.isEmpty(field)) {
                String[] str = field.split("\\s+");
                if (str.length == 2)
                    Sort.Direction.fromOptionalString(str[1]).ifPresent(direction -> {
                        query.with(Sort.by(direction, str[0]));
                    });
            }
        });

    }

    /**
     * set fields filter to request
     *
     * @param query
     * @param field
     */
    public static void applyField(Query query, Field field) {

        if (query == null || field == null)
            return;

        if (field.keySet().size() > 0) {
            List<String> includes = new ArrayList<>();
            List<String> excludes = new ArrayList<>();
            field.forEach((key, value) -> {
                boolean isInt = value instanceof Integer;
                boolean isDouble = value instanceof Double;
                boolean isLong = value instanceof Long;
                if (isInt || isDouble || isLong) {
                    int include = isInt ? (Integer) value : isDouble ? ((Double) value).intValue() : ((Long) value).intValue();
                    if (include == 0)
                        excludes.add(key);
                    else
                        includes.add(key);
                } else if (value instanceof Boolean) {
                    boolean include = (boolean) value;
                    if (include)
                        includes.add(key);
                    else
                        excludes.add(key);
                } else {
                    logger.warn("Invalid field projection {} => {}", key, value);
                }
            });
            if (includes.size() > 0)
                includes.forEach(key -> query.fields().include(key));
            else if (excludes.size() > 0)
                excludes.forEach(key -> query.fields().exclude(key));
        }



		/*if (field.getSlices() != null){
			field.getSlices().forEach((key, value ) -> {
				if (value instanceof Integer)
					query.fields().slice(key, (Integer) value);
				else if(value instanceof List) {
					List list = (List)value;
					Integer offset = 0;
					Integer size = 0;

					if(list.size() > 0 && list.get(0) instanceof Integer)
						offset = (Integer) list.get(0);

					if(list.size() > 1 && list.get(1) instanceof Integer)
						size = (Integer) list.get(1);

					query.fields().slice(key, offset, size);
				}
			});
		}

		if(field.getElemMatchs() != null ){
			field.getElemMatchs().forEach( (key, document) -> {
				query.fields().elemMatch(key, buildWhere(document));
			});
		}

		if (field.getPositionKey() != null) {
			query.fields().position(field.getPositionKey(), field.getPositionValue());
		}*/
    }

    private static final Pattern pattern = Pattern.compile("^[0-9a-fA-F]{24}$");

    public static ObjectId toObjectId(String id) {
        if (id == null) {
            return null;
        }
        Matcher matcher = pattern.matcher(id);
        if (matcher.matches()) {
            return new ObjectId(id);
        } else
            return null;
    }

    public static Object transformTimeRange(Map<String, Object> map) {

        Set<String> keys = map.keySet();
        if ((keys.contains("$gt")) || keys.contains("$lt")) {
            Object gtObject = map.get("$gt");
            Object ltObject = map.get("$lt");
            if (null != gtObject && gtObject instanceof Map) {
                Map gtMap = (Map) gtObject;
                Long gtInt = cn.hutool.core.map.MapUtil.getLong(gtMap, "$date");
                Date gtDate = new Date(gtInt);
                gtMap.remove("$date");
                map.replace("$gt", gtDate);
            }
            if (null != ltObject && ltObject instanceof Map) {
                Map ltMap = (Map) ltObject;
                Long ltInt = cn.hutool.core.map.MapUtil.getLong(ltMap, "$date");
                Date ltDate = new Date(ltInt.longValue());
                ltMap.remove("$date");
                map.replace("$lt", ltDate);
            }
            return map;
        } else {
            for (String k : keys) {
                if (map.get(k) instanceof Map) {
                    Map m = (Map) map.get(k);
                    transformTimeRange(m);
                } else if (map.get(k) instanceof List) {
                    List m = (List) map.get(k);
                    for (Object l : m) {
                        if (l instanceof Map) {
                            transformTimeRange((Map) l);
                        }
                    }
                }
            }
        }
        return map;
    }


    public static String replaceLike(String like) {
        for (String s : REGEX_CHAR) {
            if (like.contains(s)) {
                like = like.replace(s, "\\" + s);
            }
        }
        return like;
    }
}

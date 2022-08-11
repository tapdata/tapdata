package io.tapdata.pdk.apis.charset;

import io.tapdata.pdk.apis.functions.connection.CharsetResult;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.list;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class CharsetUtilsTest {

    @Test
    void testActuallyRemoveCharsetsByFilter() {
        DatabaseCharset big5 = DatabaseCharset.create().charset("big5").description("Big5 support Traditional Chinese");
        DatabaseCharset gb2312 = DatabaseCharset.create().charset("gb2312").description("GB2312 support Simplified Chinese");
        DatabaseCharset gbk = DatabaseCharset.create().charset("gbk").description("GBK support Simplified Chinese");
        DatabaseCharset utf8 = DatabaseCharset.create().charset("utf8").description("UTF-8 Unicode");
        DatabaseCharset utf8mb4 = DatabaseCharset.create().charset("utf8mb4").description("UTF-8 Unicode");
        DatabaseCharset utf16 = DatabaseCharset.create().charset("utf16").description("UTF-16 Unicode");
        DatabaseCharset hp8 = DatabaseCharset.create().charset("hp8").description("HP West European");

        List<DatabaseCharset> charsets = list(big5, gb2312, gbk, utf8, utf8mb4, utf16, hp8);

        List<CharsetCategoryFilter> list =  list(
                CharsetCategoryFilter.create()
                    .category(CharsetResult.CATEGORY_CHINESE_SIMPLIFIED)
                    .filter(".*China.*|.*Simplified.*"),
                CharsetCategoryFilter.create()
                        .category(CharsetResult.CATEGORY_CHINESE_TRADITIONAL)
                        .filter(".*China.*|.*Traditional.*"),
                CharsetCategoryFilter.create()
                        .category(CharsetResult.CATEGORY_GENERIC)
                        .filter(".*UTF.*|.*utf.*")
                );

        CharsetResult charsetResult = CharsetUtils.filterCharsets(charsets, list);

        assertNotNull(charsetResult);
        assertNotNull(charsetResult.getCharsetMap());

        charsetResult.getCharsetMap().forEach((charset,listset)->{
            System.out.println("=======>"+charset);
            for (DatabaseCharset databaseCharset : listset) {
                System.out.println("   "+databaseCharset.getCharset()+"   "+databaseCharset.getDescription());
            }
        });

        Map<String, List<DatabaseCharset>> charsetMap = new HashMap<String, List<DatabaseCharset>>(){{
            put(CharsetResult.CATEGORY_CHINESE_TRADITIONAL,new ArrayList<DatabaseCharset>(){{
                add(big5);
            }});
            put(CharsetResult.CATEGORY_CHINESE_SIMPLIFIED,new ArrayList<DatabaseCharset>(){{
                add(gb2312);
                add(gbk);
            }});
            put(CharsetResult.CATEGORY_GENERIC,new ArrayList<DatabaseCharset>(){{
                add(utf8);
                add(utf8mb4);
                add(utf16);
            }});
        }};

        assertEquals(
                CharsetResult.create().setCharsetMap(charsetMap) ,
                charsetResult,
                "succeed"
        );
    }

    @Test
    void testDefault() {
        DatabaseCharset big5 = DatabaseCharset.create().charset("big5").description("Big5 support Traditional Chinese");
        DatabaseCharset gb2312 = DatabaseCharset.create().charset("gb2312").description("GB2312 support Simplified Chinese");
        DatabaseCharset gbk = DatabaseCharset.create().charset("gbk").description("GBK support Simplified Chinese");
        DatabaseCharset utf8 = DatabaseCharset.create().charset("utf8").description("UTF-8 Unicode");
        DatabaseCharset utf8mb4 = DatabaseCharset.create().charset("utf8mb4").description("UTF-8 Unicode");
        DatabaseCharset utf16 = DatabaseCharset.create().charset("utf16").description("UTF-16 Unicode");
        DatabaseCharset hp8 = DatabaseCharset.create().charset("hp8").description("HP West European");


        List<DatabaseCharset> charsets = new ArrayList<DatabaseCharset>(){{
            add(big5);
            add(gb2312);
            add(gbk);
            add(utf8);
            add(utf8mb4);
            add(utf16);
            add(hp8);
        }};
        List<CharsetCategoryFilter> list =  new ArrayList<CharsetCategoryFilter>(){{
            add(CharsetCategoryFilter.create()
                    .category(CharsetResult.CATEGORY_CHINESE_SIMPLIFIED)
                    .filter(".*China.*|.*Simplified.*"));
            add(CharsetCategoryFilter.create()
                    .category(CharsetResult.CATEGORY_CHINESE_TRADITIONAL)
                    .filter(".*China.*|.*Traditional.*"));
            add(CharsetCategoryFilter.create()
                    .category(CharsetResult.CATEGORY_GENERIC)
                    .filter(".*UTF.*|.*utf.*"));
            add(CharsetCategoryFilter.create()
                    .category(CharsetResult.CATEGORY_DEFAULT)
                    .filter(".*"));

        }};
        CharsetResult charsetResult = CharsetUtils.filterCharsets(charsets, list);
        charsetResult.getCharsetMap().forEach((charset,listset)->{
            System.out.println("=======>"+charset);
            for (DatabaseCharset databaseCharset : listset) {
                System.out.println("   "+databaseCharset.getCharset()+"   "+databaseCharset.getDescription());
            }
        });

        Map<String, List<DatabaseCharset>> charsetMap = new HashMap<String, List<DatabaseCharset>>(){{
            put(CharsetResult.CATEGORY_CHINESE_TRADITIONAL,new ArrayList<DatabaseCharset>(){{
                add(big5);
            }});
            put(CharsetResult.CATEGORY_DEFAULT,new ArrayList<DatabaseCharset>(){{
                add(hp8);
            }});
            put(CharsetResult.CATEGORY_CHINESE_SIMPLIFIED,new ArrayList<DatabaseCharset>(){{
                add(gb2312);
                add(gbk);
            }});
            put(CharsetResult.CATEGORY_GENERIC,new ArrayList<DatabaseCharset>(){{
                add(utf8);
                add(utf8mb4);
                add(utf16);
            }});
        }};

        assertEquals(
                CharsetResult.create().setCharsetMap(charsetMap) ,
                charsetResult,
                "succeed"
        );
    }

    @Test
    void testNoDefault() {
        DatabaseCharset big5 = DatabaseCharset.create().charset("big5").description("Big5 support Traditional Chinese");
        DatabaseCharset gb2312 = DatabaseCharset.create().charset("gb2312").description("GB2312 support Simplified Chinese");
        DatabaseCharset gbk = DatabaseCharset.create().charset("gbk").description("GBK support Simplified Chinese");
        DatabaseCharset utf8 = DatabaseCharset.create().charset("utf8").description("UTF-8 Unicode");
        DatabaseCharset utf8mb4 = DatabaseCharset.create().charset("utf8mb4").description("UTF-8 Unicode");
        DatabaseCharset utf16 = DatabaseCharset.create().charset("utf16").description("UTF-16 Unicode");
        DatabaseCharset hp8 = DatabaseCharset.create().charset("hp8").description("HP West European");
        DatabaseCharset ascii = DatabaseCharset.create().charset("ascii").description("US ASCII");
        DatabaseCharset latin1 = DatabaseCharset.create().charset("latin1").description("cp1252 West European");
        List<DatabaseCharset> charsets = new ArrayList<DatabaseCharset>(){{
            add(big5);
            add(gb2312);
            add(gbk);
            add(utf8);
            add(utf8mb4);
            add(utf16);
            add(hp8);
            add(ascii);
            add(latin1);
        }};


        List<CharsetCategoryFilter> list =  new ArrayList<CharsetCategoryFilter>(){{
            add(CharsetCategoryFilter.create()
                    .category(CharsetResult.CATEGORY_CHINESE_SIMPLIFIED)
                    .filter(".*China.*|.*Simplified.*"));
            add(CharsetCategoryFilter.create()
                    .category(CharsetResult.CATEGORY_CHINESE_TRADITIONAL)
                    .filter(".*China.*|.*Traditional.*"));
            add(CharsetCategoryFilter.create()
                    .category(CharsetResult.CATEGORY_GENERIC)
                    .filter(".*UTF.*|.*utf.*"));
            add(CharsetCategoryFilter.create()
                    .category(CharsetResult.CATEGORY_ENGLISH)
                    .filter(".*ASCII.*|.*European.*"));
        }};
        CharsetResult charsetResult = CharsetUtils.filterCharsets(charsets, list);
        charsetResult.getCharsetMap().forEach((charset,listset)->{
            System.out.println("=======>"+charset);
            for (DatabaseCharset databaseCharset : listset) {
                System.out.println("   "+databaseCharset.getCharset()+"   "+databaseCharset.getDescription());
            }
        });

        Map<String, List<DatabaseCharset>> charsetMap = new HashMap<String, List<DatabaseCharset>>(){{
            put(CharsetResult.CATEGORY_CHINESE_TRADITIONAL,new ArrayList<DatabaseCharset>(){{
                add(big5);
            }});
            put(CharsetResult.CATEGORY_CHINESE_SIMPLIFIED,new ArrayList<DatabaseCharset>(){{
                add(gb2312);
                add(gbk);
            }});
            put(CharsetResult.CATEGORY_ENGLISH,new ArrayList<DatabaseCharset>(){{
                add(hp8);
                add(ascii);
                add(latin1);
            }});
            put(CharsetResult.CATEGORY_GENERIC,new ArrayList<DatabaseCharset>(){{
                add(utf8);
                add(utf8mb4);
                add(utf16);
            }});
        }};

        assertEquals(
                CharsetResult.create().setCharsetMap(charsetMap) ,
                charsetResult,
                "succeed"
        );
    }
}
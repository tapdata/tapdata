package io.tapdata.pdk.apis.charset;

import io.tapdata.pdk.apis.functions.connection.CharsetResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CharsetUtils {
    public static CharsetResult filterCharsets(List<DatabaseCharset> charsetList, List<CharsetCategoryFilter> filters) {
        Map<String, List<DatabaseCharset>> charSetMapGroup = new HashMap<>();
        charsetList.forEach(charset->{
            if ( null != charset ) {
                for (CharsetCategoryFilter characterFeatures : filters) {
                    if (null == charset.getDescription()) break;
                    if (null == characterFeatures) break;
                    if (null == characterFeatures.getFilter()) break;
                    if (null == characterFeatures.getCategory()) break;
                    if (charset.getDescription().matches(characterFeatures.getFilter())) {
                        charSetMapGroup.computeIfAbsent(characterFeatures.getCategory(), s -> new ArrayList<>()).add(charset);
                        break;
                    }
                }
            }
        });
        return CharsetResult.create().setCharsetMap(charSetMapGroup);
    }
}

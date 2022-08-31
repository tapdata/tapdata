package io.tapdata.elasticsearch.utils;

import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.metadata.AliasMetaData;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.regex.Pattern;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/28 15:38 Create
 */
public interface Indices {

    static void filterCallback(RestHighLevelClient client, String pattern, BiFunction<String, Set<AliasMetaData>, Boolean> eachFn) throws IOException {
        IndicesClient indicesClient = client.indices();
        GetAliasesRequest request = new GetAliasesRequest();
        GetAliasesResponse getAliasesResponse = indicesClient.getAlias(request, RequestOptions.DEFAULT);

        Pattern p = Pattern.compile(pattern);
        for (Map.Entry<String, Set<AliasMetaData>> en : getAliasesResponse.getAliases().entrySet()) {
            if (p.matcher(en.getKey()).find()) {
                if (!eachFn.apply(en.getKey(), en.getValue())) {
                    break;
                }
            }
        }
    }
}

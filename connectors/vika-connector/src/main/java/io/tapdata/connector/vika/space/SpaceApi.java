package io.tapdata.connector.vika.space;

import cn.vika.client.api.http.AbstractApi;
import cn.vika.client.api.http.ApiHttpClient;
import cn.vika.client.api.model.HttpResult;
import cn.vika.core.http.GenericTypeReference;
import cn.vika.core.http.HttpHeader;

public class SpaceApi extends AbstractApi {

    private static final String GET_SPACE_PATH = "/spaces";

    public SpaceApi(ApiHttpClient apiHttpClient) {
        super(apiHttpClient);
    }

    public SpaceRespone getSpaces() {

        final String path = String.format(GET_SPACE_PATH);

        HttpResult<SpaceRespone> result = getDefaultHttpClient().get(
                path, new HttpHeader(),
                new GenericTypeReference<HttpResult<SpaceRespone>>() {});
        return result.getData();
    }
}

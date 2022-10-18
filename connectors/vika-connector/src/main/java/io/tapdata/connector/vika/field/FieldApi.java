package io.tapdata.connector.vika.field;

import cn.vika.client.api.http.AbstractApi;
import cn.vika.client.api.http.ApiHttpClient;
import cn.vika.client.api.model.HttpResult;
import cn.vika.core.http.GenericTypeReference;
import cn.vika.core.http.HttpHeader;

public class FieldApi extends AbstractApi {

    private static final String GET_FIELD_PATH = "/v1/datasheets/%s/fields";

    public FieldApi(ApiHttpClient apiHttpClient) {
        super(apiHttpClient);
    }

    public FieldRespone getFields(String datasheetId) {

        final String path = String.format(GET_FIELD_PATH, datasheetId);

        HttpResult<FieldRespone> result = getDefaultHttpClient().get(
                path, new HttpHeader(),
                new GenericTypeReference<HttpResult<FieldRespone>>() {});
        return result.getData();
    }
}

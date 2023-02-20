package io.tapdata.connector.vika.view;

import cn.vika.client.api.http.AbstractApi;
import cn.vika.client.api.http.ApiHttpClient;
import cn.vika.client.api.model.*;
import cn.vika.core.http.GenericTypeReference;
import cn.vika.core.http.HttpHeader;

public class DataSheetViewApi extends AbstractApi {

    private static final String GET_DATASHEET_PATH = "/datasheets/%s/views";

    public DataSheetViewApi(ApiHttpClient apiHttpClient) {
        super(apiHttpClient);
    }

    public GetDatasheetViewRespone getViews(String datasheetId) {

        final String path = String.format(GET_DATASHEET_PATH, datasheetId);

        HttpResult<GetDatasheetViewRespone> result = getDefaultHttpClient().get(
                path, new HttpHeader(),
                new GenericTypeReference<HttpResult<GetDatasheetViewRespone>>() {});
        return result.getData();
    }
}

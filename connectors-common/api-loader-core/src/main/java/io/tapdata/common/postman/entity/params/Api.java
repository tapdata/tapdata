package io.tapdata.common.postman.entity.params;

import io.tapdata.common.support.core.emun.TapApiTag;

import java.util.Objects;

public class Api {
    public static final String PAGE_RESULT_PATH_DEFAULT_PATH = "data";
    String id;
    String name;
    String tableName;
    String pageStage;
    String pageResultPath;
    Request request;
    String response;

    public static Api create() {
        return new Api();
    }

    public Api copyOne() {
        Api api = new Api();
        api.id(this.id);
        api.name(this.name);
        api.tableName(this.tableName);
        api.pageStage(this.pageStage);
        api.pageResultPath(this.pageResultPath);
        api.request(this.request.copyOne());
        api.response(this.response);
        return api;
    }

    public String id() {
        return this.id;
    }

    public String name() {
        return this.name;
    }

    public String tableName() {
        return this.tableName;
    }

    public String pageStage() {
        return this.pageStage;
    }

    public String pageResultPath() {
        return this.pageResultPath;
    }

    public Request request() {
        return this.request;
    }

    public String response() {
        return this.response;
    }

    public Api id(String id) {
        this.id = id;
        return this;
    }

    public Api name(String name) {
        this.name = name;
        return this;
    }

    public Api nameFullDetail(String name) {
        this.name = name;
        this.tableName = TapApiTag.analysisTableName(name);
        this.pageStage = TapApiTag.getPageStage(name);
        if (Objects.nonNull(this.pageStage)) {
            String[] arr = this.pageStage.split(":");
            if (arr.length == 2) {
                this.pageResultPath = arr[1];
            }
            if (arr.length > 0) {
                this.pageStage = arr[0];
            }
        }
        return this;
    }

    public Api tableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public Api pageStage(String pageStage) {
        this.pageStage = pageStage;
        return this;
    }

    public Api pageResultPath(String pageResultPath) {
        this.pageResultPath = pageResultPath;
        return this;
    }

    public Api request(Request request) {
        this.request = request;
        return this;
    }

    public Api response(String response) {
        this.response = response;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Api api = (Api) o;
        return Objects.equals(id, api.id) && Objects.equals(name, api.name) && Objects.equals(request, api.request) && Objects.equals(response, api.response);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, request, response);
    }
}

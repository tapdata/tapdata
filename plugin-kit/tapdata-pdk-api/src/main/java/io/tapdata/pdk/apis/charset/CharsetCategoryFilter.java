package io.tapdata.pdk.apis.charset;

public class CharsetCategoryFilter {
    public static CharsetCategoryFilter create() {
        return new CharsetCategoryFilter();
    }
    private String category;
    public CharsetCategoryFilter category(String category) {
        this.category = category;
        return this;
    }
    private String filter;
    public CharsetCategoryFilter filter(String filter) {
        this.filter = filter;
        return this;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }
}

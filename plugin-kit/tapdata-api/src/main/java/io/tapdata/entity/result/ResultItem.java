package io.tapdata.entity.result;

public class ResultItem {
    public ResultItem() {}

    public ResultItem(String item, int result, String information) {
        this.item = item;
        this.result = result;
        this.information = information;
    }

    /**
     * Test item, like connection test, username and password test, etc
     */
    private String item;

    /**
     * Test result, pass or failed
     */
    private int result;
    /**
     * Information about why it failed
     */
    private String information;

    public String getItem() {
        return item;
    }

    public void setItem(String item) {
        this.item = item;
    }

    public int getResult() {
        return result;
    }

    public void setResult(int result) {
        this.result = result;
    }

    public String getInformation() {
        return information;
    }

    public void setInformation(String information) {
        this.information = information;
    }

    @Override
    public String toString() {
        return ResultItem.class.getSimpleName() + " item " + item +
                " result " + result +
                " information " + information;
    }
}

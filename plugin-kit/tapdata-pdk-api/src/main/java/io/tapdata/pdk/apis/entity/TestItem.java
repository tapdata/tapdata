package io.tapdata.pdk.apis.entity;

public class TestItem {
    /**
     * Check whether the connector support the version.
     */
    public static final String ITEM_VERSION = "Version";
    /**
     * Check is connection available.
     */
    public static final String ITEM_CONNECTION = "Connection";
    /**
     * Check can login.
     */
    public static final String ITEM_LOGIN = "Login";
    /**
     * Check permission for whether read is supported.
     */
    public static final String ITEM_READ = "Read";
    /**
     * Check permission for whether write is supported.
     */
    public static final String ITEM_WRITE = "Write";
    /**
     * Check permission for whether read log is supported.
     */
//    public static final String ITEM_CONNECTION = "Connection login";
//    public static final String ITEM_VERSION = "DataSource version";
//    public static final String ITEM_LOGIN = "Login";
//    public static final String ITEM_READ = "Check Read privilege";
//    public static final String ITEM_WRITE = "Check Write privilege";
    public static final String ITEM_READ_LOG = "Read log";

    public TestItem(String item, int result, String information) {
        this.item = item;
        this.result = result;
        this.information = information;
    }

    /**
     * Test item, like connection test, username and password test, etc
     */
    private String item;
    public static final int RESULT_SUCCESSFULLY = 1;
    public static final int RESULT_SUCCESSFULLY_WITH_WARN = 2;
    public static final int RESULT_FAILED = 10;
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

    public boolean isSuccess() {
        return result == RESULT_SUCCESSFULLY || result == RESULT_SUCCESSFULLY_WITH_WARN;
    }

    @Override
    public String toString() {
        return TestItem.class.getSimpleName() + " item " + item +
                " result " + result +
                " information " + information;
    }
}

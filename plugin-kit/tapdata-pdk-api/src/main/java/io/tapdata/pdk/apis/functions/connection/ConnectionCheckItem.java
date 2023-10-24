package io.tapdata.pdk.apis.functions.connection;

public class ConnectionCheckItem {
    /**
     * Ping server by establishing connection to tcp/udp port and the time takes when connection established.
     */
    public static final String ITEM_PING = "Ping";
    /**
     * Use JDBC/others API connects to server and the time takes to succeed.
     */
    public static final String ITEM_CONNECTION = "Connection";

    public static ConnectionCheckItem create() {
        return new ConnectionCheckItem();
    }

    /**
     * takes milliseconds to check.
     */
    private Long takes;
    public ConnectionCheckItem takes(long takes) {
        this.takes = takes;
        return this;
    }

    /**
     * Test item, like connection test, username and password test, etc
     */
    private String item;
    public ConnectionCheckItem item(String item) {
        this.item = item;
        return this;
    }
    public static final int RESULT_SUCCESSFULLY = 1;
    public static final int RESULT_SUCCESSFULLY_WITH_WARN = 2;
    public static final int RESULT_FAILED = 10;
    /**
     * Test result, pass or failed
     */
    private int result;
    public ConnectionCheckItem result(int result) {
        this.result = result;
        return this;
    }
    /**
     * Information about why it failed
     */
    private String information;
    public ConnectionCheckItem information(String information) {
        this.information = information;
        return this;
    }


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

    public Long getTakes() {
        return takes;
    }

    public void setTakes(Long takes) {
        this.takes = takes;
    }

    @Override
    public String toString() {
        return ConnectionCheckItem.class.getSimpleName() + " item " + item +
                " result " + result +
                " takes " + takes +
                " information " + information;
    }
}

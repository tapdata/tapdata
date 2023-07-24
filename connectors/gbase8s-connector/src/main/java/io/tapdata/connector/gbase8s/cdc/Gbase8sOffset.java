package io.tapdata.connector.gbase8s.cdc;

/**
 * Author:Skeet
 * Date: 2023/6/16
 **/
public class Gbase8sOffset {
    private Long lastScn;
    private Long pendingScn;

    public Gbase8sOffset(){
    }

    public Long getPendingScn() {
        return pendingScn;
    }

    public void setPendingScn(Long pendingScn) {
        this.pendingScn = pendingScn;
    }

    public Long getLastScn() {
        return lastScn;
    }

    public void setLastScn(Long lastScn) {
        this.lastScn = lastScn;
    }
}

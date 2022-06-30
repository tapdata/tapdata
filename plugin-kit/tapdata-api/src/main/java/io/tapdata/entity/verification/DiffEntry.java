package io.tapdata.entity.verification;

/**
 * The difference for each map key.
 */
public class DiffEntry {
    public static DiffEntry create(String key) {
        return new DiffEntry().key(key);
    }

    /**
     * map key for comparison
     */
    private String key;
    public DiffEntry key(String key) {
        this.key = key;
        return this;
    }

    /**
     * value from left map
     */
    private Object left;
    public DiffEntry left(Object left) {
        this.left = left;
        return this;
    }

    /**
     * missing key from left map
     */
    private Boolean missingOnLeft;
    public DiffEntry missingOnLeft(Boolean missingOnLeft) {
        this.missingOnLeft = missingOnLeft;
        return this;
    }

    /**
     * value from right map
     */
    private Object right;
    public DiffEntry right(Object right) {
        this.right = right;
        return this;
    }

    /**
     * missing key from right map
     */
    private Boolean missingOnRight;
    public DiffEntry missingOnRight(Boolean missingOnRight) {
        this.missingOnRight = missingOnRight;
        return this;
    }

    public Object getLeft() {
        return left;
    }

    public void setLeft(Object left) {
        this.left = left;
    }

    public Object getRight() {
        return right;
    }

    public void setRight(Object right) {
        this.right = right;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Boolean getMissingOnLeft() {
        return missingOnLeft;
    }

    public void setMissingOnLeft(Boolean missingOnLeft) {
        this.missingOnLeft = missingOnLeft;
    }

    public Boolean getMissingOnRight() {
        return missingOnRight;
    }

    public void setMissingOnRight(Boolean missingOnRight) {
        this.missingOnRight = missingOnRight;
    }
}

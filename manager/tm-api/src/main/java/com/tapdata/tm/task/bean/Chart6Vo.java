package com.tapdata.tm.task.bean;

import lombok.Builder;
import lombok.Data;

import java.math.BigInteger;

@Data
@Builder
public class Chart6Vo {
    private BigInteger outputTotal;
    private BigInteger inputTotal;
    private BigInteger insertedTotal;
    private BigInteger updatedTotal;
    private BigInteger deletedTotal;

    public boolean empty() {
        return BigInteger.ZERO.equals(outputTotal) &&
                BigInteger.ZERO.equals(inputTotal) &&
                BigInteger.ZERO.equals(insertedTotal) &&
                BigInteger.ZERO.equals(updatedTotal) &&
                BigInteger.ZERO.equals(deletedTotal);
    }
}

package com.tapdata.tm.user.entity;

import lombok.Data;

@Data
public class Notification {
    private Connected connected=new Connected();
    private ConnectionInterrupted connectionInterrupted=new ConnectionInterrupted();
    private StoppedByError stoppedByError=new StoppedByError();


    @Override
    public String toString() {
        return "Notification";
    }
}





package com.tapdata.tm.task.utils;

public class TaskServiceUtils {

    public static double getTransformProcess(int total, int finished){
        if (finished != 0) {
            double process = finished / (total * 1d);
            if (process > 1) {
                process = 1;
            }
            return ((int) (process * 100)) / 100d;
        }
        return 0;
    }
}

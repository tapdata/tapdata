package io.tapdata.pdk.core.utils;

import java.util.Random;

public class RandomDraw {
    private Random random = new Random();
    private int[] array;

    public RandomDraw(int count) {
        array = new int[count];
        for (int i = 0; i < count; i++)
            array[i] = i;
    }

    public int next() {
        if (array.length <= 0)
            return -1;
        int index = random.nextInt(array.length);
        int value = array[index];
        int[] newArray = new int[array.length - 1];
        if (index == 0) {
            System.arraycopy(array, 1, newArray, 0, newArray.length);
        } else if (index == array.length - 1) {
            System.arraycopy(array, 0, newArray, 0, newArray.length);
        } else {
            System.arraycopy(array, 0, newArray, 0, index);
            System.arraycopy(array, index + 1, newArray, index, newArray.length - index);
        }
        array = newArray;
        return value;
    }
}
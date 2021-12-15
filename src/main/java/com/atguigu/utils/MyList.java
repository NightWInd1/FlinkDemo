package com.atguigu.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author nightwind
 */
public class MyList {
    public  static <T>List<T> toList(Iterable<T> it){
        List<T> list = new ArrayList<>();
        for (T t : it) {
            list.add(t);
        }
        return list;
    }
}

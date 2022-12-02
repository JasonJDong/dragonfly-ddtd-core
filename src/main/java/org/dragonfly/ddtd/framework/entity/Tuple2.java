package org.dragonfly.ddtd.framework.entity;

import java.io.Serializable;

/**
 * 作用描述 二元组
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/11/25
 **/
public class Tuple2<T1, T2> implements Serializable {

    /**
     * 数据1
     */
    private T1 t1;
    /**
     * 数据2
     */
    private T2 t2;

    public Tuple2(T1 t1, T2 t2) {
        this.t1 = t1;
        this.t2 = t2;
    }

    public T1 getT1() {
        return t1;
    }

    public void setT1(T1 t1) {
        this.t1 = t1;
    }

    public T2 getT2() {
        return t2;
    }

    public void setT2(T2 t2) {
        this.t2 = t2;
    }

    /**
     * 新二元组对象
     * @param t1 数据1
     * @param t2 数据2
     * @param <T1> 数据1类型
     * @param <T2> 数据2类型
     * @return 二元组对象
     */
    public static <T1, T2> Tuple2<T1, T2> newOne(T1 t1, T2 t2) {
        return new Tuple2<>(t1, t2);
    }
}

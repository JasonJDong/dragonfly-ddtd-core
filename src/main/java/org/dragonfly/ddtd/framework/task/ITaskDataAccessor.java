package org.dragonfly.ddtd.framework.task;

/**
 * 作用描述 任务数据访问器
 *
 * @param <T> 数据对象类型
 * @author jian.dong1
 * @version 1.0
 * @date 2022/1/27
 **/
public interface ITaskDataAccessor<T> {
    /**
     * 获取可缓存任务数据访问器
     * @return 可缓存任务数据访问器
     */
    ITaskCacheableDataAccessor<T> getTaskCacheableDataAccessor();

    /**
     * 获取可持久化数据访问器
     * @return 可持久化数据访问器
     */
    ITaskPersistDataAccessor<T> getTaskPersistDataAccessor();
}

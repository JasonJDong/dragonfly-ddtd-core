package org.dragonfly.ddtd.framework.task;

import org.dragonfly.ddtd.framework.entity.ITaskContext;

import javax.annotation.Nullable;
import java.util.Collection;

/**
 * 作用描述 分布式的任务数据访问器
 * <p>
 * 通常来说是访问KV数据库，如redis、memcache
 * </p>
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/1/28
 **/
public interface ITaskCacheableDataAccessor<T> {
    /**
     * 是否能进行分布式访问
     *
     * @return 是否是分布式的
     */
    default boolean isDistributed() {
        return true;
    }

    /**
     * 缓存所有任务数据
     *
     * @param context 任务上下文
     * @param data    所有任务数据
     * @throws Exception 缓存异常
     */
    void cacheAllData(ITaskContext context, Collection<T> data) throws Exception;

    /**
     * 是否存在任务数据
     *
     * @param context 任务上下文
     * @return 是否存在任务数据
     */
    boolean exists(ITaskContext context);

    /**
     * 获取一项任务数据
     *
     * @param context 任务上下文
     * @return 一项任务数据
     */
    @Nullable T pollOne(ITaskContext context);

    /**
     * 清理所有数据
     *
     * @param context 任务上下文
     */
    void clear(ITaskContext context);
}

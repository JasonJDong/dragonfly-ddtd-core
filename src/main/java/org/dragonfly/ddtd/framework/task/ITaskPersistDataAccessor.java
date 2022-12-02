package org.dragonfly.ddtd.framework.task;

import org.dragonfly.ddtd.framework.entity.ITaskContext;
import org.dragonfly.ddtd.enums.TaskWorkState;

import javax.annotation.Nullable;
import java.util.Collection;

/**
 * 作用描述 分布式的任务数据访问器
 * <p>
 * 通常来说是访问数据库，如Mysql、PostgreSql等
 * </p>
 *
 * @param <T> 任务数据对象类型
 * @author jian.dong1
 * @version 1.0
 * @date 2022/1/28
 **/
public interface ITaskPersistDataAccessor<T> {

    /**
     * 持久化任务数据
     *
     * @param context 任务上下文
     * @param data    任务数据
     */
    void persist(ITaskContext context, Collection<T> data);

    /**
     * 中止剩余任务
     *
     * @param context 任务上下文
     */
    void terminate(ITaskContext context);

    /**
     * 已完成的任务
     *
     * @param context 任务上下文
     * @param one     一条数据
     * @param state   任务处理结果
     */
    void resolved(ITaskContext context, @Nullable T one, TaskWorkState state);

    /**
     * 存在未完成任务
     *
     * @param context 任务上下文
     * @return 是否存在未完成任务
     */
    boolean existsUnresolved(ITaskContext context);

    /**
     * 未处理完的数据
     *
     * @param context 任务上下文
     * @return 未处理完的数据
     */
    Collection<T> allUnresolved(ITaskContext context);

    /**
     * 清理所有数据
     *
     * @param context 任务上下文
     */
    void clear(ITaskContext context);
}

package org.dragonfly.ddtd.framework.lifecycle;

import org.dragonfly.ddtd.framework.entity.ITaskContext;
import org.dragonfly.ddtd.framework.task.ITaskWorker;

import javax.annotation.Nullable;

/**
 * 作用描述 任务生命周期钩子
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/2/11
 **/
public interface ITaskLifeCycleHook {

    /**
     * 任务开始
     * <p>
     * 任务开始，会仅调用一次
     * </p>
     *
     * @param taskWorker  任务处理器
     * @param taskContext 任务上下文
     */
    default void onTaskStarted(ITaskWorker<?> taskWorker, ITaskContext taskContext) {
    }

    /**
     * 每个子任务开始
     * <p>
     * 每个子任务（JVM或线程）开始执行时调用
     * </p>
     *  @param taskWorker  任务处理器
     * @param taskContext 任务上下文
     * @param taskData    任务数据
     */
    default void onEachTaskStarted(ITaskWorker<?> taskWorker, ITaskContext taskContext, @Nullable Object taskData) {
    }

    /**
     * 每个子任务执行异常时调用
     * <p>
     * 每个子任务（JVM或线程）开始执行时调用
     * </p>
     *
     * @param taskWorker  任务处理器
     * @param taskContext 任务上下文
     * @param error       异常
     */
    default void onEachTaskError(ITaskWorker<?> taskWorker, ITaskContext taskContext, Exception error) {
    }

    /**
     * 每个子任务执行完成时调用
     * <p>
     * 每个子任务（JVM或线程）开始执行时调用
     * </p>
     *
     * @param taskWorker  任务处理器
     * @param taskContext 任务上下文
     */
    default void onEachTaskCompleted(ITaskWorker<?> taskWorker, ITaskContext taskContext) {
    }

    /**
     * 所有任务完成时调用
     *
     * @param taskWorker  任务处理器
     * @param taskContext 任务上下文
     */
    default void onTaskCompleted(ITaskWorker<?> taskWorker, ITaskContext taskContext) {
    }
}

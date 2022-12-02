package org.dragonfly.ddtd.framework.task;

import org.dragonfly.ddtd.framework.entity.ITaskContext;

import java.util.Collection;

/**
 * 作用描述 任务工厂
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/1/27
 **/
public interface ITaskFactory<T> {

    /**
     * 启动任务
     * @param context 任务上下文
     * @param data 所有任务数据
     * @throws Exception 启动异常
     */
    void applyTask(ITaskContext context, Collection<T> data) throws Exception;

    /**
     * 任务中止
     * @param worker 子任务
     * @param context 任务上下文
     * @return 是否中止
     */
    boolean terminateTask(ITaskWorker<T> worker, ITaskContext context);
}

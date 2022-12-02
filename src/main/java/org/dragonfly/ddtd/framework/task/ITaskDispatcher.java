package org.dragonfly.ddtd.framework.task;

import org.dragonfly.ddtd.framework.entity.ITaskContext;

import java.util.Collection;

/**
 * 作用描述 任务调度器
 *
 * @param <T> 任务数据对象类型
 * @author jian.dong1
 * @version 1.0
 * @date 2022/1/27
 **/
public interface ITaskDispatcher<T> {
    /**
     * 开始任务调度
     *
     * @param taskWorker 子任务执行者
     * @param context    任务上下文
     * @param data       所有数据
     * @throws Exception 启动异常
     */
    void startDispatch(ITaskWorker<T> taskWorker, ITaskContext context, Collection<T> data) throws Exception;

    /**
     * 中止任务
     *
     * @param taskWorker 任务处理器
     * @param context    任务上下文
     */
    void terminate(ITaskWorker<T> taskWorker, ITaskContext context);
}

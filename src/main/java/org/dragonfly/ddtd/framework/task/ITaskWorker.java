package org.dragonfly.ddtd.framework.task;

import org.dragonfly.ddtd.framework.entity.ITaskContext;
import org.dragonfly.ddtd.enums.TaskWorkState;

/**
 * 作用描述 任务处理
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/1/27
 **/
public interface ITaskWorker<T> extends ITaskMeta {

    /**
     * 是否接受任务
     *
     * @param context 任务上下文
     * @return 是否接受任务
     */
    boolean accept(ITaskContext context);

    /**
     * 获取任务数据访问器
     * @return 任务数据访问器
     */
    ITaskDataAccessor<T> getTaskDataAccessor();

    /**
     * 上下文转换
     *
     * @param rawContext 原始字符串类型数据
     * @return 上下文对象
     */
    ITaskContext parseContext(String rawContext);

    /**
     * 执行子任务
     *
     * @param context 任务上下文
     * @param data    业务数据
     * @return 执行结果
     */
    TaskWorkState partitionWork(ITaskContext context, T data);

    /**
     * 任务清理
     *
     * @param context 任务上下文
     * @return 执行结果
     */
    TaskWorkState cleanupWork(ITaskContext context);
}

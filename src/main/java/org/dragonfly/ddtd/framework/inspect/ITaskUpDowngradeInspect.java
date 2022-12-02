package org.dragonfly.ddtd.framework.inspect;

import org.dragonfly.ddtd.framework.entity.ITaskContext;
import org.dragonfly.ddtd.framework.task.ITaskWorker;

/**
 * 作用描述 任务升降级检测
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/1/28
 **/
public interface ITaskUpDowngradeInspect {
    /**
     * 是否是分布式模式
     *
     * @param taskWorker 当前任务处理器
     * @param context    任务上下文
     * @return 是否是分布式模式
     */
    boolean isDistributed(ITaskWorker<?> taskWorker, ITaskContext context);
}

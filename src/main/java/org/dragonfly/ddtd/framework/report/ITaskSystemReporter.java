package org.dragonfly.ddtd.framework.report;

import org.dragonfly.ddtd.framework.entity.ITaskContext;
import org.dragonfly.ddtd.framework.task.ITaskWorker;

/**
 * 作用描述 系统报告接口
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/1/28
 **/
public interface ITaskSystemReporter {
    /**
     * 报告
     *
     * @param baseMsg     基础信息
     * @param taskWorker  当前任务处理器
     * @param taskContext 任务上下文
     * @param stage       处理阶段
     * @param extendObjs  扩展数据
     */
    void report(String baseMsg, ITaskWorker<?> taskWorker, ITaskContext taskContext, TaskProcessStage stage, Object... extendObjs);

    /**
     * 报告
     *
     * @param baseMsg     基础信息
     * @param taskWorker  当前任务处理器
     * @param taskContext 任务上下文
     * @param stage       处理阶段
     * @param error       错误
     * @param extendObjs  扩展数据
     */
    void report(String baseMsg, ITaskWorker<?> taskWorker, ITaskContext taskContext, TaskProcessStage stage, Exception error, Object... extendObjs);
}

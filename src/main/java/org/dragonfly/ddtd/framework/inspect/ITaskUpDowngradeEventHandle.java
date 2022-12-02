package org.dragonfly.ddtd.framework.inspect;

import org.dragonfly.ddtd.framework.entity.ITaskContext;
import org.dragonfly.ddtd.framework.task.ITaskWorker;

/**
 * 作用描述：任务升降级事件处理
 *
 * @author dongjian
 * @date 2022/2/9 - 10:35 下午
 */
public interface ITaskUpDowngradeEventHandle {

    /**
     * 注册升级通知回调
     *
     * @param callback 回调
     */
    void registerUpDowngrade(Object callback);

    /**
     * 取消注册升级通知回调
     *
     * @param callback 回调
     */
    void unregisterUpDowngrade(Object callback);

    /**
     * 发送通知
     *
     * @param taskWorker 当前任务处理器
     * @param context    任务上下文
     */
    void emitUpgrade(ITaskWorker<?> taskWorker, ITaskContext context);

    /**
     * 发送通知
     *
     * @param taskWorker 当前任务处理器
     * @param context    任务上下文
     */
    void emitDowngrade(ITaskWorker<?> taskWorker, ITaskContext context);
}

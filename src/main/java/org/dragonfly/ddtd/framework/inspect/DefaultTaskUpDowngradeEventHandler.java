package org.dragonfly.ddtd.framework.inspect;

import org.dragonfly.ddtd.framework.entity.ITaskContext;
import org.dragonfly.ddtd.framework.task.ITaskWorker;
import org.dragonfly.ddtd.framework.task.TaskDowngradeData;
import org.dragonfly.ddtd.framework.task.TaskUpgradeData;
import com.google.common.eventbus.AsyncEventBus;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 作用描述：任务升降级事件处理器
 *
 * @author dongjian
 * @date 2022/2/9 - 10:37 下午
 */
public class DefaultTaskUpDowngradeEventHandler implements ITaskUpDowngradeEventHandle {

    /**
     * CachedThreadPool
     */
    private final ExecutorService eventEmitterThreadPool =
        new ThreadPoolExecutor(0,
            Integer.MAX_VALUE,
            60L, TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new ThreadFactoryBuilder().setNameFormat("UpDowngrade Process - %s").setDaemon(true).build());

    private final AsyncEventBus eventBus = new AsyncEventBus(eventEmitterThreadPool);

    @Override
    public void registerUpDowngrade(Object callback) {
        this.eventBus.register(callback);
    }

    @Override
    public void unregisterUpDowngrade(Object callback) {
        this.eventBus.unregister(callback);
    }

    @Override
    public void emitUpgrade(ITaskWorker<?> taskWorker, ITaskContext context) {
        this.eventBus.post(
            new TaskUpgradeData(context)
        );
    }

    @Override
    public void emitDowngrade(ITaskWorker<?> taskWorker, ITaskContext context) {
        this.eventBus.post(
            new TaskDowngradeData(context)
        );
    }
}

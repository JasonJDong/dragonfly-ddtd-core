package org.dragonfly.ddtd.framework.task;

import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.dragonfly.ddtd.conf.GlobalProperties;
import org.dragonfly.ddtd.framework.TaskZooKeeperFactory;
import org.dragonfly.ddtd.framework.entity.ITaskContext;
import org.dragonfly.ddtd.framework.inspect.DefaultTaskUpDowngradeEventHandler;
import org.dragonfly.ddtd.framework.inspect.ITaskInspect;
import org.dragonfly.ddtd.framework.inspect.ITaskUpDowngradeEventHandle;
import org.dragonfly.ddtd.framework.lifecycle.ITaskLifeCycleHook;
import org.dragonfly.ddtd.framework.report.ITaskReporter;
import org.dragonfly.ddtd.framework.report.TaskProcessStage;

import javax.annotation.Nullable;
import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.Map;

/**
 * 作用描述 分布式任务工厂
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/1/28
 **/
@Slf4j
public class DefaultTaskFactory<T> implements ITaskFactory<T> {

    private final GlobalProperties globalProperties;

    private final TaskZooKeeperFactory taskZooKeeperFactory;

    private final ITaskWorker<T> taskWorker;

    private final ITaskInspect taskInspect;

    private final ITaskReporter taskReporter;

    private final ITaskUpDowngradeEventHandle taskUpDowngradeEventHandle = new DefaultTaskUpDowngradeEventHandler();
    /**
     * 保存入口调度器
     */
    private final Map<String, ITaskDispatcher<T>> entranceDispatcherHolder = Maps.newConcurrentMap();
    /**
     * 任务处理完成时需要回调清理必要资源
     */
    private final ITaskLifeCycleHook taskLifeCycleHook = new InternalTaskLifeCycleHook();

    /**
     * 任务处理回调
     */
    @Getter
    @Setter
    private ITaskLifeCycleHook taskHandleCallback;
    /**
     * 分布式调度器，初始化并启用
     */
    private ITaskDispatcher<T> perpetualDispatcher;


    public DefaultTaskFactory(GlobalProperties globalProperties,
                              TaskZooKeeperFactory taskZooKeeperFactory,
                              ITaskWorker<T> taskWorker,
                              ITaskInspect taskInspect,
                              ITaskReporter taskReporter) {
        this.globalProperties = globalProperties;
        this.taskZooKeeperFactory = taskZooKeeperFactory;
        this.taskWorker = taskWorker;
        this.taskInspect = taskInspect;
        this.taskReporter = taskReporter;
        this.taskUpDowngradeEventHandle.registerUpDowngrade(this);
    }

    class InternalTaskLifeCycleHook implements ITaskLifeCycleHook {

        private final WeakReference<Map<String, ITaskDispatcher<T>>> reference = new WeakReference<>(
                entranceDispatcherHolder
        );

        @Override
        public void onTaskCompleted(ITaskWorker<?> taskWorker, ITaskContext taskContext) {
            if (taskContext == null) {
                taskReporter.getSystemReporter().report(
                        "任务完成异常，任务上下文丢失",
                        taskWorker,
                        null,
                        TaskProcessStage.COMPLETED_ERROR,
                        new IllegalArgumentException()
                );
                return;
            }
            Map<String, ITaskDispatcher<T>> holder = this.reference.get();
            if (holder != null) {
                holder.remove(taskContext.getTaskId());
            }
            if (getTaskHandleCallback() != null) {
                getTaskHandleCallback().onTaskCompleted(taskWorker, taskContext);
            }
        }

        @Override
        public void onTaskStarted(ITaskWorker<?> taskWorker, ITaskContext taskContext) {
            if (getTaskHandleCallback() != null) {
                getTaskHandleCallback().onTaskStarted(taskWorker, taskContext);
            }
        }

        @Override
        public void onEachTaskStarted(ITaskWorker<?> taskWorker, ITaskContext taskContext, @Nullable Object taskData) {
            if (getTaskHandleCallback() != null) {
                getTaskHandleCallback().onEachTaskStarted(taskWorker, taskContext, taskData);
            }
        }

        @Override
        public void onEachTaskError(ITaskWorker<?> taskWorker, ITaskContext taskContext, Exception error) {
            if (getTaskHandleCallback() != null) {
                getTaskHandleCallback().onEachTaskError(taskWorker, taskContext, error);
            }
        }

        @Override
        public void onEachTaskCompleted(ITaskWorker<?> taskWorker, ITaskContext taskContext) {
            if (getTaskHandleCallback() != null) {
                getTaskHandleCallback().onEachTaskCompleted(taskWorker, taskContext);
            }
        }
    }

    @Subscribe
    public void onUpgrade(TaskUpgradeData data) {

        // 获取未处理完成的数据
        Collection<T> allUnresolved =
                this.taskWorker.getTaskDataAccessor().getTaskPersistDataAccessor().allUnresolved(data.getTaskContext());
        if (CollectionUtils.isEmpty(allUnresolved)) {
            taskReporter.getSystemReporter().report(
                    "升级后无需要处理任务数据",
                    taskWorker,
                    data.getTaskContext(),
                    TaskProcessStage.UPGRADE
            );
            return;
        }
        taskReporter.getSystemReporter().report(
                "准备进行升级数据处理",
                taskWorker,
                data.getTaskContext(),
                TaskProcessStage.UPGRADE
        );
        this.entranceDispatcherHolder.put(data.getTaskContext().getTaskId(), this.perpetualDispatcher);
        try {
            this.perpetualDispatcher.startDispatch(this.taskWorker, data.getTaskContext(), allUnresolved);
        } catch (Exception e) {
            taskReporter.getSystemReporter().report(
                    "升级失败",
                    taskWorker,
                    data.getTaskContext(),
                    TaskProcessStage.UPGRADE,
                    e
            );
        }
    }

    @Subscribe
    public void onDowngrade(TaskDowngradeData data) {

        ITaskDispatcher<?> dispatcher = this.entranceDispatcherHolder.get(data.getTaskContext().getTaskId());
        // 如果为null，说明当前降级方案不在当前JVM处理
        if (dispatcher == null) {
            log.info("当前非主调度器，退出：{}", Thread.currentThread().getName());
            return;
        }
        // 清理掉当前调度器
        log.info("降级事件：{}，factory hashCode：{}，dispatchers：{}", Thread.currentThread().getName(), this.hashCode(), entranceDispatcherHolder);
        this.entranceDispatcherHolder.remove(data.getTaskContext().getTaskId());

        // 获取未处理完成的数据
        Collection<T> allUnresolved =
                this.taskWorker.getTaskDataAccessor().getTaskPersistDataAccessor().allUnresolved(data.getTaskContext());
        if (CollectionUtils.isEmpty(allUnresolved)) {
            taskReporter.getSystemReporter().report(
                    "降级后无需要处理任务数据",
                    taskWorker,
                    data.getTaskContext(),
                    TaskProcessStage.DOWNGRADE
            );
            return;
        }
        TaskLocalDispatcher<T> localDispatcher = new TaskLocalDispatcher<>(
                this.globalProperties,
                this.taskReporter,
                this.taskInspect,
                this.taskUpDowngradeEventHandle, taskLifeCycleHook);

        this.entranceDispatcherHolder.put(data.getTaskContext().getTaskId(), localDispatcher);
        try {
            taskReporter.getSystemReporter().report(
                    "准备进行降级数据处理",
                    taskWorker,
                    data.getTaskContext(),
                    TaskProcessStage.DOWNGRADE
            );
            localDispatcher.startDispatch(this.taskWorker, data.getTaskContext(), allUnresolved);
        } catch (Exception e) {
            taskReporter.getSystemReporter().report(
                    "降级失败",
                    taskWorker,
                    data.getTaskContext(),
                    TaskProcessStage.DOWNGRADE,
                    e
            );
        }
    }

    @Override
    public void applyTask(ITaskContext context, Collection<T> data) throws Exception {
        try {
            ITaskDispatcher<T> taskDispatcher = this.getTaskDispatcher(this.taskWorker, context);
            if (CollectionUtils.isNotEmpty(data)) {
                // 只保存主处理器
                this.entranceDispatcherHolder.put(context.getTaskId(), taskDispatcher);
            }
            taskReporter.getSystemReporter().report(
                    "开始任务",
                    taskWorker,
                    context,
                    TaskProcessStage.START
            );
            taskDispatcher.startDispatch(this.taskWorker, context, data);

        } catch (Exception e) {
            taskReporter.getSystemReporter().report(
                    "启动任务异常",
                    taskWorker,
                    context,
                    TaskProcessStage.START,
                    e
            );
            throw e;
        }
    }

    /**
     * 获取任务调度器
     * <p>
     * 如果是分布式环境，则将直接返回初始化好的
     * </p>
     *
     * @param taskWorker 任务处理器
     * @param context    任务上下文
     * @return 任务调度器
     */
    private ITaskDispatcher<T> getTaskDispatcher(ITaskWorker<T> taskWorker, ITaskContext context) {

        // 根据当前任务环境，返回分布式或者本地任务调度器
        boolean distributed = this.taskInspect.getTaskUpDowngradeInspect().isDistributed(taskWorker, context);
        if (distributed && this.perpetualDispatcher == null) {
            throw new IllegalArgumentException("请调用initialize进行初始化");
        }
        return distributed ?
                this.perpetualDispatcher :
                new TaskLocalDispatcher<>(
                        this.globalProperties,
                        this.taskReporter,
                        this.taskInspect,
                        this.taskUpDowngradeEventHandle,
                        this.taskLifeCycleHook);
    }

    @Override
    public boolean terminateTask(ITaskWorker<T> worker, ITaskContext context) {
        for (Map.Entry<String, ITaskDispatcher<T>> entry : entranceDispatcherHolder.entrySet()) {
            if (StringUtils.equals(entry.getKey(), context.getTaskId())) {
                ITaskDispatcher<T> dispatcher = entry.getValue();
                try {
                    if (dispatcher != null) {
                        dispatcher.terminate(worker, context);
                        taskReporter.getSystemReporter().report(
                                "中止任务",
                                taskWorker,
                                context,
                                TaskProcessStage.TERMINATED
                        );
                    }
                } catch (Exception e) {
                    taskReporter.getSystemReporter().report(
                            "中止任务异常",
                            taskWorker,
                            context,
                            TaskProcessStage.TERMINATED,
                            e
                    );
                }
            }
        }
        return false;
    }

    /**
     * Clean the task resources.
     * @throws Exception might exists exceptions.
     */
    public void destroy() throws Exception {
        this.taskUpDowngradeEventHandle.unregisterUpDowngrade(this);
    }

    /**
     * Start the zookeeper listener to watch task create.
     * @throws Exception any exceptions.
     */
    public void initialize() throws Exception {

        if (this.perpetualDispatcher == null) {
            final TaskDistributionDispatcher<T> dispatcher = new TaskDistributionDispatcher<>(
                    this.taskZooKeeperFactory,
                    this.taskInspect,
                    this.taskReporter,
                    this.taskUpDowngradeEventHandle,
                    this.taskLifeCycleHook);
            dispatcher.startListening(this.taskWorker);
            this.perpetualDispatcher = dispatcher;
        }
    }
}

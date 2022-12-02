package org.dragonfly.ddtd.framework.task;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.dragonfly.ddtd.conf.GlobalProperties;
import org.dragonfly.ddtd.enums.TaskWorkState;
import org.dragonfly.ddtd.framework.entity.ITaskContext;
import org.dragonfly.ddtd.framework.entity.Tuple2;
import org.dragonfly.ddtd.framework.inspect.ITaskInspect;
import org.dragonfly.ddtd.framework.inspect.ITaskUpDowngradeEventHandle;
import org.dragonfly.ddtd.framework.lifecycle.EmptyTaskLifeCycleHooker;
import org.dragonfly.ddtd.framework.lifecycle.ITaskLifeCycleHook;
import org.dragonfly.ddtd.framework.report.ITaskReporter;
import org.dragonfly.ddtd.framework.report.TaskProcessStage;
import org.dragonfly.ddtd.framework.thread.ParallelTaskExecutor;
import org.dragonfly.ddtd.framework.thread.ParallelTaskMonitor;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 作用描述 基于本地缓存任务调度
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/1/28
 **/
@Slf4j
public class TaskLocalDispatcher<T> implements ITaskDispatcher<T> {

    private final GlobalProperties properties;

    private final ITaskReporter taskReporter;

    private final ITaskCacheableDataAccessor<T> taskDataAccessor;

    private final ITaskInspect taskInspect;

    private final ITaskUpDowngradeEventHandle taskUpDowngradeEventHandle;

    private final ITaskLifeCycleHook taskLifeCycleHook;

    public TaskLocalDispatcher(GlobalProperties properties,
                               ITaskReporter taskReporter,
                               ITaskInspect taskInspect,
                               ITaskUpDowngradeEventHandle taskUpDowngradeEventHandle) {
        this(properties, taskReporter, taskInspect, taskUpDowngradeEventHandle, new EmptyTaskLifeCycleHooker());
    }

    public TaskLocalDispatcher(GlobalProperties properties,
                               ITaskReporter taskReporter,
                               ITaskInspect taskInspect,
                               ITaskUpDowngradeEventHandle taskUpDowngradeEventHandle,
                               ITaskLifeCycleHook taskLifeCycleHook) {
        Preconditions.checkNotNull(properties, "properties不能为空");
        Preconditions.checkNotNull(properties.getDowngrade(), "降级配置不能为空，请查看dragonfly-ddtd.downgrade");
        Preconditions.checkNotNull(taskReporter, "taskReporter不能为空");
        Preconditions.checkNotNull(taskUpDowngradeEventHandle, "taskUpDowngradeEventHandle不能为空");
        Preconditions.checkNotNull(taskLifeCycleHook, "taskLifeCycleHook不能为空");
        this.properties = properties;
        this.taskDataAccessor = new TaskLocalDataAccessor<>();
        this.taskInspect = taskInspect;
        this.taskReporter = taskReporter;
        this.taskUpDowngradeEventHandle = taskUpDowngradeEventHandle;
        this.taskLifeCycleHook = taskLifeCycleHook;
    }

    @Override
    public void startDispatch(ITaskWorker<T> taskWorker, ITaskContext context, Collection<T> data) throws Exception {

        if (CollectionUtils.isEmpty(data)) {
            // 如果没有任何数据，无法进行处理，直接忽略
            return;
        }

        this.taskLifeCycleHook.onTaskStarted(taskWorker, context);

        // 保存数据
        try {
            taskWorker.getTaskDataAccessor().getTaskPersistDataAccessor().persist(context, data);
            // 存储任务数据
            this.taskDataAccessor.cacheAllData(context, data);
            taskReporter.getSystemReporter().report(
                    "（本地处理）已经保存任务数据",
                    taskWorker,
                    context,
                    TaskProcessStage.PREPARE
            );
        } catch (Exception e) {
            taskReporter.getSystemReporter().report(
                    "（本地处理）任务数据无法保存，创建任务失败",
                    taskWorker,
                    context,
                    TaskProcessStage.PREPARE
            );
            throw e;
        }

        TaskMonitorHandler handler = new TaskMonitorHandler(taskReporter, taskWorker, context);
        ParallelTaskMonitor.registerMonitorHandler(handler);

        List<Tuple2<String, ? extends Callable<Void>>> tasks = Lists.newArrayListWithExpectedSize(this.properties.getDowngrade().getThreadsCount());

        AtomicBoolean upgrade = new AtomicBoolean(false);

        Callable<Void> taskHandler = getTaskHandler(taskWorker, context, upgrade);

        for (int i = 0; i < this.properties.getDowngrade().getThreadsCount(); i++) {
            tasks.add(Tuple2.newOne(taskWorker.getName(), taskHandler));
        }

        // 同步执行完成
        ParallelTaskExecutor.executesDone(tasks, this.properties.getDowngrade().getTimeoutSeconds());

        // 所有线程完成后判断是否需要升级
        // 已经跳出循环，存在未处理完成任务且有升级标记，需要升级处理
        if (taskWorker.getTaskDataAccessor().getTaskPersistDataAccessor().existsUnresolved(context) &&
                upgrade.get()) {
            // 清理本地缓存
            this.taskDataAccessor.clear(context);
            taskReporter.getSystemReporter().report(
                    "（本地处理）升级",
                    taskWorker,
                    context,
                    TaskProcessStage.UPGRADE
            );
            this.taskUpDowngradeEventHandle.emitUpgrade(taskWorker, context);
        } else {
            // 清理处理
            this.cleanupHandle(taskWorker, context);
            this.taskLifeCycleHook.onTaskCompleted(taskWorker, context);
        }
    }

    private Callable<Void> getTaskHandler(ITaskWorker<T> taskWorker, ITaskContext context, AtomicBoolean upgrade) {
        return () -> {
            while (this.taskDataAccessor.exists(context)) {
                T taskData = null;
                try {
                    /*
                     * 升级检测
                     * 1、如果此处需要升级，则退出循环
                     * 2、通知升级
                     */
                    if (this.taskInspect.getTaskUpDowngradeInspect().isDistributed(taskWorker, context)) {
                        upgrade.compareAndSet(false, true);
                        break;
                    }

                    taskReporter.getSystemReporter().report(
                            "（本地处理）子任务开始执行",
                            taskWorker,
                            context,
                            TaskProcessStage.EACH_START
                    );
                    // 当accept为false时，不会进行等待，减少再调一次exit
                    boolean accept;
                    if (!(accept = taskWorker.accept(context))) {
                        TimeUnit.MILLISECONDS.sleep(this.properties.getPauseScanMills());
                    }
                    // 等待过程中，任务执行完毕
                    if (!accept && !this.taskDataAccessor.exists(context)) {
                        taskReporter.getSystemReporter().report(
                                "（本地处理）所有任务已完成，当前线程退出",
                                taskWorker,
                                context,
                                TaskProcessStage.PARTITION_COMPLETED
                        );
                        break;
                    }
                    // 这里可能接受任务
                    if (accept || taskWorker.accept(context)) {

                        taskData = this.taskDataAccessor.pollOne(context);
                        this.taskLifeCycleHook.onEachTaskStarted(taskWorker, context, taskData);

                        // 处理子任务
                        TaskWorkState taskWorkState = taskWorker.partitionWork(
                                context,
                                taskData
                        );
                        taskWorker.getTaskDataAccessor().getTaskPersistDataAccessor().resolved(
                                context,
                                taskData,
                                taskWorkState);
                        taskReporter.getSystemReporter().report(
                                "（本地处理）子任务执行结果已保存",
                                taskWorker,
                                context,
                                TaskProcessStage.EACH_COMPLETED
                        );

                        this.taskLifeCycleHook.onEachTaskCompleted(taskWorker, context);
                    }

                } catch (Exception e) {
                    if (e instanceof InterruptedException) {
                        taskReporter.getSystemReporter().report(
                                "（本地处理）子任务执行被终止",
                                taskWorker,
                                context,
                                TaskProcessStage.EACH_ERROR,
                                e
                        );
                    } else {
                        taskReporter.getSystemReporter().report(
                                "（本地处理）子任务执行异常",
                                taskWorker,
                                context,
                                TaskProcessStage.EACH_COMPLETED,
                                e,
                                taskData
                        );
                        this.taskLifeCycleHook.onEachTaskError(taskWorker, context, e);
                        if (this.taskDataAccessor.exists(context)) {
                            // 如果仍然需要执行子任务，那么报告完当前任务异常后继续执行
                            continue;
                        }
                    }
                    break;
                }
            }

            return null;
        };
    }

    /**
     * 清理处理
     *
     * @param taskWorker 任务处理器
     */
    private void cleanupHandle(ITaskWorker<T> taskWorker,
                               ITaskContext context) {
        ParallelTaskExecutor.executeAsync(() -> {
            try {
                taskReporter.getSystemReporter().report(
                        "（本地处理）清理任务线程开始",
                        taskWorker,
                        context,
                        TaskProcessStage.COMPLETED
                );
                TaskWorkState taskWorkState = taskWorker.cleanupWork(context);
                taskReporter.getSystemReporter().report(
                        "（本地处理）清理任务线程结束",
                        taskWorker,
                        context,
                        TaskProcessStage.EXIT
                );
            } catch (Exception e) {
                if (e.getCause() instanceof InterruptedException) {
                    taskReporter.getSystemReporter().report(
                            "（本地处理）清理任务线程中断",
                            taskWorker,
                            context,
                            TaskProcessStage.COMPLETED_ERROR,
                            e
                    );
                } else {
                    taskReporter.getSystemReporter().report(
                            "（本地处理）清理任务线程异常",
                            taskWorker,
                            context,
                            TaskProcessStage.COMPLETED_ERROR,
                            e
                    );
                }
            }
            return null;
        }, taskWorker.getName());
    }

    @Override
    public void terminate(ITaskWorker<T> taskWorker, ITaskContext context) {
        // 中止任务将通过处理任务数据实现
        taskReporter.getSystemReporter().report(
                "（本地处理）中止任务",
                taskWorker,
                context,
                TaskProcessStage.TERMINATED
        );
        taskWorker.getTaskDataAccessor().getTaskPersistDataAccessor().terminate(context);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
}

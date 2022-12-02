package org.dragonfly.ddtd.framework.task;

import com.google.common.base.Preconditions;
import org.dragonfly.ddtd.framework.entity.ITaskContext;
import org.dragonfly.ddtd.framework.report.ITaskReporter;
import org.dragonfly.ddtd.framework.report.TaskProcessStage;
import org.dragonfly.ddtd.framework.thread.ParallelTaskMonitor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;

import java.lang.ref.WeakReference;
import java.util.Objects;

/**
 * 作用描述 任务监控处理
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/2/8
 **/
@Slf4j
@Getter
public class TaskMonitorHandler implements ParallelTaskMonitor.ICxMonitorHandler {

    private final ITaskReporter taskReporter;

    private final ITaskWorker<?> taskWorker;

    private final WeakReference<ITaskContext> taskContext;

    TaskMonitorHandler(ITaskReporter taskReporter, ITaskWorker<?> taskWorker, ITaskContext taskContext) {
        Preconditions.checkNotNull(taskReporter, "报告器不能为空");
        Preconditions.checkNotNull(taskWorker, "任务处理器不能为空");
        Preconditions.checkNotNull(taskContext, "任务上下文不能为空");
        this.taskReporter = taskReporter;
        this.taskWorker = taskWorker;
        this.taskContext = new WeakReference<>(taskContext);
    }

    @Override
    public long getExceedTimeMs() {
        // 每个任务都需要监控
        return 0;
    }

    @Override
    public String getTaskName() {
        ITaskWorker<?> taskWorker = getTaskWorker();
        if (taskWorker != null) {
            return taskWorker.getName();
        }
        return null;
    }

    @Override
    public boolean invalid() {
        return this.getTaskContext().get() == null;
    }

    @Override
    public void handle(ParallelTaskMonitor.MonitorMeta meta, long elapsed) {
        ITaskContext taskContext = this.getTaskContext().get();
        if (taskContext == null) {
            return;
        }
        taskReporter.getSystemReporter().report(
                String.format("任务执行进度追踪，名称：%s，调用者：%s，任务批次号：%s，任务id；%s，线程：%s，已执行时间：%s",
                        meta.getTaskName(),
                        meta.getCaller(),
                        taskContext.getTaskId(),
                        meta.getTaskId(),
                        meta.getThreadName(),
                        DurationFormatUtils.formatDurationHMS(elapsed < 0 || elapsed >= Long.MAX_VALUE ? 0 : elapsed)
                ),
                taskWorker,
                taskContext,
                TaskProcessStage.EACH_EXECUTING,
                meta,
                elapsed
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TaskMonitorHandler)) {
            return false;
        }
        TaskMonitorHandler handler = (TaskMonitorHandler) obj;
        ITaskContext handlerContext = handler.getTaskContext().get();
        ITaskContext thisHandlerContext = getTaskContext().get();
        if (handlerContext == null || thisHandlerContext == null) {
            return false;
        }
        return StringUtils.equals(handlerContext.getTaskId(), thisHandlerContext.getTaskId());
    }

    @Override
    public int hashCode() {

        ITaskContext taskContext = this.getTaskContext().get();
        if (taskContext == null) {
            return super.hashCode();
        }
        return Objects.hash(taskContext.getTaskId());
    }
}

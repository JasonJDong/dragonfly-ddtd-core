package org.dragonfly.ddtd.framework.inspect;

import com.google.common.base.Preconditions;
import org.dragonfly.ddtd.framework.TaskZooKeeperFactory;

import javax.annotation.Nonnull;

/**
 * 作用描述 任务检测器
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/2/8
 **/
public class DefaultTaskInspector implements ITaskInspect {

    private final DefaultTaskUpDowngradeInspector taskUpDowngradeInspector;

    private final DefaultTaskPerformanceInspect taskPerformanceInspect;

    public DefaultTaskInspector(TaskZooKeeperFactory taskZooKeeperFactory) {
        Preconditions.checkNotNull(taskZooKeeperFactory, "taskZooKeeperFactory 不能为空");
        this.taskUpDowngradeInspector = new DefaultTaskUpDowngradeInspector(taskZooKeeperFactory);
        this.taskPerformanceInspect = new DefaultTaskPerformanceInspect(taskZooKeeperFactory.getProperties());
    }

    @Nonnull
    @Override
    public ITaskUpDowngradeInspect getTaskUpDowngradeInspect() {
        return taskUpDowngradeInspector;
    }

    @Nonnull
    @Override
    public ITaskPerformanceInspect getTaskPerformanceInspect() {
        return taskPerformanceInspect;
    }
}

package org.dragonfly.ddtd.framework.inspect;

import javax.annotation.Nonnull;

/**
 * 作用描述 任务检测
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/2/8
 **/
public interface ITaskInspect {
    /**
     * 获取升降级检测
     * @return 升降级检测
     */
    @Nonnull
    ITaskUpDowngradeInspect getTaskUpDowngradeInspect();

    /**
     * 获取性能检测
     * @return 性能检测
     */
    @Nonnull
    ITaskPerformanceInspect getTaskPerformanceInspect();
}

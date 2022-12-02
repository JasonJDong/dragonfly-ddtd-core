package org.dragonfly.ddtd.framework.entity;

import org.dragonfly.ddtd.enums.TaskDistributionStrategy;

/**
 * 作用描述 任务策略
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/2/8
 **/
public interface ITaskStrategy {
    /**
     * 获取任务策略
     * @return 任务策略
     */
    TaskDistributionStrategy getStrategy();

    /**
     * 获取固定实例数量
     * @return 固定实例数量
     */
    Integer getFixedCount();
}

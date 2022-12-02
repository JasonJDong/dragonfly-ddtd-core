package org.dragonfly.ddtd.framework.task;

import lombok.Data;

/**
 * 作用描述 任务节点载荷数据
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/2/8
 **/
@Data
class TaskNodeData {

    /**
     * 任务分布式策略
     */
    private String distributionStrategy;
    /**
     * 固定模式数量
     */
    private Integer fixedCount;
    /**
     * 业务数据
     */
    private String data;
}

package org.dragonfly.ddtd.framework.report;

/**
 * 作用描述 任务处理阶段
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/2/17
 **/
public enum TaskProcessStage {
    /**
     * 进入
     */
    ENTRANCE,
    /**
     * 准备
     */
    PREPARE,
    /**
     * 任务开始
     */
    START,
    /**
     * 升级
     */
    UPGRADE,
    /**
     * 降级
     */
    DOWNGRADE,
    /**
     * 中止任务
     */
    TERMINATED,
    /**
     * 子任务开始
     */
    EACH_START,
    /**
     * 子任务执行
     */
    EACH_EXECUTING,
    /**
     * 子任务结束
     */
    EACH_COMPLETED,
    /**
     * 子任务异常
     */
    EACH_ERROR,
    /**
     * 当前分区完成
     */
    PARTITION_COMPLETED,
    /**
     * 当前分区异常
     */
    PARTITION_ERROR,
    /**
     * 任务完成
     */
    COMPLETED,
    /**
     * 任务完成时异常
     */
    COMPLETED_ERROR,
    /**
     * 退出
     */
    EXIT
    ;
}

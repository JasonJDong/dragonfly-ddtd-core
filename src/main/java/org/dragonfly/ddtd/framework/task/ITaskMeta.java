package org.dragonfly.ddtd.framework.task;

/**
 * 作用描述 任务元数据
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/1/27
 **/
public interface ITaskMeta {
    /**
     * 获取版本
     * @return 版本
     */
    String getVersion();

    /**
     * 获取任务名称
     * <p>
     *     NOTE: 如果相同任务类型，但属于不同任务对象，需要设置任务名称唯一
     * </p>
     * @return 任务名称
     */
    String getName();

    /**
     * 获取任务描述
     * @return 任务描述
     */
    String getDescription();
}

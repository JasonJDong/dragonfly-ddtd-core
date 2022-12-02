package org.dragonfly.ddtd.framework.entity;

/**
 * 作用描述 任务上下文
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/1/27
 **/
public interface ITaskContext extends ITaskStrategy {
    /**
     * 任务Id，必须是唯一的
     *
     * @return 任务Id
     */
    String getTaskId();
}

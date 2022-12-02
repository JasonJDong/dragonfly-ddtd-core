package org.dragonfly.ddtd.framework.report;

/**
 * 作用描述 任务报告
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/1/28
 **/
public interface ITaskReporter {
    /**
     * 获取系统报告器
     * @return 系统报告器
     */
    ITaskSystemReporter getSystemReporter();
}

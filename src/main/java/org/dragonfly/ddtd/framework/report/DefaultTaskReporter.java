package org.dragonfly.ddtd.framework.report;

/**
 * 作用描述 默认报告器
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/2/8
 **/
public class DefaultTaskReporter implements ITaskReporter {

    private final ITaskSystemReporter taskSystemReporter = new DefaultTaskSystemReporter();

    @Override
    public ITaskSystemReporter getSystemReporter() {
        return taskSystemReporter;
    }
}

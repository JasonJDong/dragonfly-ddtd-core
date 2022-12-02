package org.dragonfly.ddtd.framework.inspect;

/**
 * 作用描述 任务性能检查
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/2/8
 **/
public interface ITaskPerformanceInspect {
    /**
     * 性能正常
     * @return 是否正常
     */
    boolean isNormal();
}

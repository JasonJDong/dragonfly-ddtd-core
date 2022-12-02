package org.dragonfly.ddtd.conf;

import lombok.Data;

/**
 * 作用描述 性能属性
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/2/16
 **/
@Data
public class PerformanceProperties {
    /**
     * cpu负载率
     */
    private int cpuLoad = 80;
    /**
     * 内存负载率
     */
    private int memoryLoad = 80;
}

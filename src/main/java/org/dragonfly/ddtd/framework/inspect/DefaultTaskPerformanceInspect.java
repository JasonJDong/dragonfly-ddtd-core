package org.dragonfly.ddtd.framework.inspect;

import org.dragonfly.ddtd.conf.GlobalProperties;
import org.dragonfly.ddtd.utils.RuntimeUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 作用描述 性能检测默认实现
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/2/8
 **/
@Slf4j
@RequiredArgsConstructor
public class DefaultTaskPerformanceInspect implements ITaskPerformanceInspect {

    private final GlobalProperties globalProperties;

    @Override
    public boolean isNormal() {
        boolean normal =  globalProperties.getPerformance().getCpuLoad() > RuntimeUtil.cpuLoad()
                || globalProperties.getPerformance().getMemoryLoad() > RuntimeUtil.memoryLoad();
        if (!normal) {
            log.warn("性能检测当前实例负载较高，{}，{}", RuntimeUtil.cpuLoadString(), RuntimeUtil.memoryLoadString());
        }
        return normal;
    }
}

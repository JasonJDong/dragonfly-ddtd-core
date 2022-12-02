package org.dragonfly.ddtd.conf;

import lombok.Data;
import lombok.Getter;

/**
 * 作用描述 降级方案配置
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/1/28
 **/
@Data
public class DowngradeProperties {
    /**
     * 是否开启降级方案
     */
    private boolean enabled = true;
    /**
     * 降级后是否升级
     */
    private boolean allowUpgrade = true;
    /**
     * 是否自动升级
     */
    private boolean autoUpgrade = true;
    /**
     * 降级执行任务线程个数
     */
    private int threadsCount = 8;
    /**
     * 任务超时时间
     */
    private int timeoutSeconds = 1800;
}

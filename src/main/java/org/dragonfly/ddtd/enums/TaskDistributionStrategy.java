package org.dragonfly.ddtd.enums;

import org.dragonfly.ddtd.conf.GlobalProperties;

/**
 * 作用描述 分布式任务策略
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/1/25
 **/
public enum TaskDistributionStrategy {
    /**
     * 动态调整
     * <p>
     *     当前模式将根据给定判断条件，判断是否当前实例可否接受任务<br/>
     *     如果不可接受任务，将通过{@link GlobalProperties#getPauseScanMills()}间隔
     *     扫描任务数据和判断条件，来继续接受任务或者退出
     * </p>
     */
    DYNAMIC,
    /**
     * 固定数量
     * <p>
     *     当前模式，会在分布式锁环境下，依次接受任务，直到创建的接受任务的实例达到指定数量<br/>
     *     当前模式不能暂停<br/>
     *     当前模式下如果被中止，不会新建实例接替
     * </p>
     */
    FIXED
}

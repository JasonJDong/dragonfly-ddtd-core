package org.dragonfly.ddtd.conf;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

/**
 * 作用描述 配置
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/1/25
 **/
@Data
public class GlobalProperties {
    /**
     * ZK 地址
     */
    private String zookeeperAddr;
    /**
     * 命名空间
     */
    private String namespace;
    /**
     * 分组
     */
    private String group = "default";
    /**
     * 任务暂停扫描间隔
     */
    private long pauseScanMills = 30000;
    /**
     * ZooKeeper重连基本等待间隔
     */
    private int baseRetrySleepTime = 1000;
    /**
     * 重连尝试次数
     */
    private int retryCount = 3;
    /**
     * 启动时是否检查ZK准备好
     */
    private boolean startingCheck = true;
    /**
     * 降级方案
     */
    private DowngradeProperties downgrade = new DowngradeProperties();
    /**
     * 性能配置
     */
    private PerformanceProperties performance = new PerformanceProperties();

    public void valid() throws Exception{
        if (StringUtils.isBlank(getZookeeperAddr())){
            throw new IllegalStateException("未配置ZooKeeper服务器：dragonfly-ddtd.zookeeper-addr");
        }
        if (StringUtils.isBlank(getNamespace())){
            throw new IllegalStateException("未配置ZooKeeper所用根节点：dragonfly-ddtd.namespace");
        }
        if (StringUtils.isBlank(getGroup())){
            throw new IllegalStateException("未配置ZooKeeper所用根节点分组，请调用setGroup或设置配置dragonfly-ddtd.group");
        }
    }
}

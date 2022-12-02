package org.dragonfly.ddtd.framework;

import com.google.common.base.Preconditions;
import org.dragonfly.ddtd.conf.GlobalProperties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import static org.apache.curator.framework.imps.CuratorFrameworkState.STARTED;

/**
 * 作用描述 动态分布式任务调度工厂
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/1/25
 **/
@Slf4j
public class TaskZooKeeperFactory implements AutoCloseable {

    @Getter
    private final GlobalProperties properties;

    private CuratorFramework client;
    /**
     * 是否已经初始化，避免多次调用
     * 导致使用client的调用方丢失连接
     */
    private boolean initialized;
    /**
     * 根节点路径
     */
    private String rootNodePath;

    public TaskZooKeeperFactory(GlobalProperties properties) {
        Preconditions.checkNotNull(properties, "动态分布式任务调度无配置，请检查dragonfly-ddtd下的配置");
        this.properties = properties;
    }

    public String getRootNodePath() {
        if (StringUtils.isBlank(this.rootNodePath)) {
            this.rootNodePath = String.format("/%s_dynamic_task_%s", this.properties.getNamespace(), this.properties.getGroup());
        }
        return this.rootNodePath;
    }

    private void createRootNode() throws Exception {
        if (this.client.getState() == STARTED) {
            try {
                Stat stat = this.client.checkExists()
                        .forPath(getRootNodePath());
                if (stat == null) {

                    this.client.create()
                            .withMode(CreateMode.PERSISTENT)
                            .forPath(getRootNodePath());
                }
            } catch (Exception e) {
                log.error("创建动态分布式任务调度ZooKeeper根节点失败", e);
                throw e;
            }
        }
    }

    /**
     * 获取ZooKeeper连接客户端
     *
     * @return 客户端
     */
    public CuratorFramework getClient() {
        Preconditions.checkNotNull(this.client, "ZooKeeper连接客户端尚未初始化");
        Preconditions.checkArgument(this.client.getState() == STARTED, "ZooKeeper连接客户端未连接或未连接成功");
        return client;
    }

    @Override
    public void close() throws Exception {
        if (!this.initialized){
            return;
        }
        this.destroy();
    }

    public void destroy() throws Exception {
        log.info("系统清理，关闭ZooKeeper连接");
        this.initialized = false;
        this.rootNodePath = null;
        CloseableUtils.closeQuietly(this.client);
    }

    public void initialize() throws Exception {
        if (this.initialized) {
            return;
        }
        this.initialized = true;
        this.properties.valid();

        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(
                properties.getBaseRetrySleepTime(),
                properties.getRetryCount()
        );
        this.client = CuratorFrameworkFactory.newClient(
                properties.getZookeeperAddr(),
                retry
        );
        this.client.start();
        this.client.usingNamespace(properties.getNamespace());
        if (properties.isStartingCheck() && this.client.getState() != STARTED) {
            throw new IllegalStateException("动态分布式任务调度连接ZooKeeper时失败");
        }
        this.createRootNode();
    }

    /**
     * 重连
     * <p>
     * 通常在不启动检查时，需要连接调用
     * </p>
     *
     * @throws Exception 异常
     */
    public void reconnect() throws Exception {
        if (this.client == null) {
            this.initialized = false;
            this.initialize();
        }
        if (this.client.getState() != STARTED) {
            this.client.start();
        }
        if (this.client.getState() != STARTED) {
            throw new IllegalStateException("动态分布式任务调度连接ZooKeeper时失败");
        }
    }
}

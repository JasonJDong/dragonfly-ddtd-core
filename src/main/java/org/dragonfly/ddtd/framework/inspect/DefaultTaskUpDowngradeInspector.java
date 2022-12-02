package org.dragonfly.ddtd.framework.inspect;

import org.dragonfly.ddtd.framework.TaskZooKeeperFactory;
import org.dragonfly.ddtd.framework.entity.ITaskContext;
import org.dragonfly.ddtd.framework.task.ITaskWorker;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 作用描述 升降级控制默认实现
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/1/28
 **/
@Slf4j
public class DefaultTaskUpDowngradeInspector implements ITaskUpDowngradeInspect {

    private final TaskZooKeeperFactory taskZooKeeperFactory;

    /**
     * 默认分布式
     */
    private final AtomicBoolean distributed = new AtomicBoolean(true);

    public DefaultTaskUpDowngradeInspector(TaskZooKeeperFactory taskZooKeeperFactory) {
        this.taskZooKeeperFactory = taskZooKeeperFactory;
        this.zooKeeperConnectionStateListen();
    }

    @Override
    public boolean isDistributed(ITaskWorker<?> taskWorker, ITaskContext context) {
        // ZK正常
        return distributed.get() &&
                // 获取任务数据中间件正常
                taskWorker.getTaskDataAccessor().getTaskCacheableDataAccessor().isDistributed();
    }

    /**
     * 监控ZooKeeper连接状态
     */
    private void zooKeeperConnectionStateListen() {
        InternalConnectionStateListener listener = new InternalConnectionStateListener(distributed);
        taskZooKeeperFactory.getClient().getConnectionStateListenable().addListener(listener);
    }

    /**
     * ZooKeeper连接状态监控
     */
    static class InternalConnectionStateListener implements ConnectionStateListener {

        private final AtomicBoolean connected;

        InternalConnectionStateListener(AtomicBoolean connected) {
            this.connected = connected;
        }

        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState) {
            connected.set(newState.isConnected());
        }
    }
}

package org.dragonfly.ddtd.locker;

import com.google.common.base.Preconditions;
import org.dragonfly.ddtd.framework.TaskZooKeeperFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.Locker;
import org.apache.curator.utils.ZKPaths;

import java.util.concurrent.TimeUnit;

/**
 * 作用描述 分布式锁
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/1/26
 **/
public class ZooKeeperLocker implements AutoCloseable {

    public static final String LOCKS_PATH = "/locks";

    private final Locker locker;

    /**
     * 阻塞获取锁
     *
     * @param factory  ZooKeeper工厂
     * @param locker   锁定器
     * @param timeout  超时时间
     * @param timeUnit 时间单位
     * @throws Exception 异常
     */
    public ZooKeeperLocker(TaskZooKeeperFactory factory, String locker, long timeout, TimeUnit timeUnit) throws Exception {
        Preconditions.checkNotNull(factory, "factory不能为空");
        Preconditions.checkArgument(StringUtils.isNotBlank(locker), "locker不能为空");
        // 创建锁
        String lockPath = ZKPaths.makePath(
                factory.getProperties().getNamespace(),
                factory.getProperties().getGroup(),
                LOCKS_PATH,
                locker
        );
        InterProcessMutex mutex = new InterProcessMutex(factory.getClient(), lockPath);
        if (timeout == -1) {
            this.locker = new Locker(mutex);
        } else {
            this.locker = new Locker(mutex, timeout, timeUnit);
        }
    }

    /**
     * 阻塞获取锁
     *
     * @param factory ZooKeeper工厂
     * @param locker  锁定器
     * @throws Exception 异常
     */
    public ZooKeeperLocker(TaskZooKeeperFactory factory, String locker) throws Exception {
        this(factory, locker, -1L, null);
    }

    @Override
    public void close() throws Exception {
        if (this.locker != null) {
            this.locker.close();
        }
    }
}

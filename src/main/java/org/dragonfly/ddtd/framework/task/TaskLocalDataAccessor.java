package org.dragonfly.ddtd.framework.task;

import org.dragonfly.ddtd.framework.entity.ITaskContext;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Queues;
import org.apache.commons.collections4.CollectionUtils;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.Queue;

/**
 * 作用描述 本地数据访问
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/1/28
 **/
public class TaskLocalDataAccessor<T> implements ITaskCacheableDataAccessor<T> {

    /**
     * 任务数据
     */
    private final Cache<String, Queue<?>> taskAllData = CacheBuilder.newBuilder()
            .expireAfterWrite(Duration.ofDays(1))
            .build();

    @Override
    public void cacheAllData(ITaskContext context, Collection<T> data) throws Exception {
        taskAllData.put(context.getTaskId(), Queues.newConcurrentLinkedQueue(data));
    }

    @Override
    public boolean exists(ITaskContext context) {
        try {
            return CollectionUtils.isNotEmpty(taskAllData.getIfPresent(context.getTaskId()));
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean isDistributed() {
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T pollOne(ITaskContext context) {
        return (T)Optional.ofNullable(taskAllData.getIfPresent(context.getTaskId()))
                .map(Queue::poll)
                .orElse(null);
    }

    @Override
    public void clear(ITaskContext context) {
        taskAllData.invalidate(context.getTaskId());
    }
}

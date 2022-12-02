package org.dragonfly.ddtd.framework.thread;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 作用描述 并发任务监控
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2021/11/8
 **/
@Slf4j
public final class ParallelTaskMonitor {

    /**
     * 线程安全的监控数据
     */
    private final static ConcurrentSkipListSet<MonitorMeta> monitorMetas = new ConcurrentSkipListSet<>();
    /**
     * 监控线程池
     */
    private final static ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(
            1,
            new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("Parallel task monitor daemon - %s").build(),
            new ThreadPoolExecutor.DiscardPolicy()
    );

    /**
     * 监控处理句柄
     */
    private final static List<ICxMonitorHandler> handlers = Lists.newCopyOnWriteArrayList();
    /**
     * 忽略1秒内完成的数据
     */
    private final static ICxMonitorHandler defaultIgnoreFastDoneTask = new ICxMonitorHandler() {
        @Override
        public long getExceedTimeMs() {
            return TimeUnit.SECONDS.toMillis(1L);
        }

        @Override
        public String getTaskName() {
            return null;
        }

        @Override
        public boolean invalid() {
            return false;
        }

        @Override
        public void handle(MonitorMeta meta, long elapsed) {
            // do nothing
        }
    };

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            handlers.forEach(ParallelTaskMonitor::unregisterMonitorHandler);
            executor.shutdown();
        }));
        executor.scheduleAtFixedRate(new MonitorTask(monitorMetas), 0L, 5L, TimeUnit.SECONDS);
    }

    /**
     * 添加监控数据
     * 该方法需要足够简单，不要添加其他方法
     *
     * @param meta 监控数据
     */
    public static void add(MonitorMeta meta) {
        monitorMetas.add(meta);
    }

    /**
     * 注册监控句柄
     *
     * @param handler 监控处理句柄
     */
    public static void registerMonitorHandler(ICxMonitorHandler handler) {
        if (handlers.contains(handler)) {
            return;
        }
        handlers.add(handler);
    }

    /**
     * 取消注册监控句柄
     *
     * @param handler 监控处理句柄
     */
    public static void unregisterMonitorHandler(ICxMonitorHandler handler) {
        handlers.remove(handler);
    }

    /**
     * 监控任务
     */
    @RequiredArgsConstructor
    private static class MonitorTask implements Runnable {

        private final ConcurrentSkipListSet<MonitorMeta> monitorMetas;

        @Override
        public void run() {
            // 移除已完成的方法
            if (CollectionUtils.isEmpty(monitorMetas)) {
                return;
            }
            // 没有外部处理器，忽略所有任务监控
            if (CollectionUtils.isEmpty(handlers)) {
                return;
            }
            long now = System.currentTimeMillis();
            // 获取处理时间超过指定时间的

            try {

                Map<MonitorMeta, List<ICxMonitorHandler>> forRemoved = Maps.newHashMap();
                for (ICxMonitorHandler handler : handlers) {
                    this.monitorMetas.stream()
                            .filter(meta -> handler.getExceedTimeMs() <= 0 || now - meta.getStartAt().getTime() >= handler.getExceedTimeMs())
                            .filter(meta -> StringUtils.isBlank(handler.getTaskName()) || StringUtils.equalsIgnoreCase(handler.getTaskName(), meta.getTaskName()))
                            .forEach(meta -> {
                                List<ICxMonitorHandler> forRemovedHandlers = forRemoved.computeIfAbsent(meta, (m) -> Lists.newArrayList());
                                if (!handler.isForever()) {
                                    forRemovedHandlers.add(handler);
                                }
                                handler.handle(meta, calcElapsed(meta, now));
                            });
                }

                // 删除已失效的处理器
                handlers.removeIf(ICxMonitorHandler::invalid);

                // 删除已完成的任务
                monitorMetas.removeIf(meta -> {

                    if (meta.isDone()) {
                        List<ICxMonitorHandler> forRemovedHandlers = forRemoved.get(meta);
                        if (CollectionUtils.isNotEmpty(forRemovedHandlers)) {
                            forRemovedHandlers.forEach(handlers::remove);
                        }
                    }
                    return false;
                });

            } catch (Exception e) {
                log.error("监控任务处理结果执行异常", e);
            }
        }

        private long calcElapsed(MonitorMeta meta, long now) {
            if (meta.getEndAt() == null) {
                return now - meta.getStartAt().getTime();
            }
            return meta.getEndAt().getTime() - meta.getStartAt().getTime();
        }
    }

    public interface ICxMonitorHandler {
        /**
         * 获取超时时间，单位：毫秒
         *
         * @return 超时时间
         */
        long getExceedTimeMs();

        /**
         * 获取任务名称
         *
         * @return 任务名称
         */
        String getTaskName();

        /**
         * 是否永续监控
         *
         * @return 默认非永续
         */
        default boolean isForever() {
            return false;
        }

        /**
         * 是否已失效
         *
         * @return 是否已失效
         */
        default boolean invalid() {
            return false;
        }

        /**
         * 处理
         *
         * @param meta    元数据
         * @param elapsed 耗时
         */
        void handle(MonitorMeta meta, long elapsed);
    }

    @Builder
    @EqualsAndHashCode(callSuper = false)
    @Getter
    public static class MonitorMeta implements Comparable<MonitorMeta> {
        /**
         * 调用者
         */
        private String caller;
        /**
         * 线程名称
         */
        private String threadName;
        /**
         * 任务名称
         */
        private String taskName;
        /**
         * 任务Id
         */
        private String taskId;
        /**
         * 开始时间
         */
        private Date startAt;
        /**
         * 结束时间
         */
        @Setter
        private Date endAt;
        /**
         * 异常
         */
        @Setter
        private Exception exception;

        /**
         * 是否任务完成
         */
        public boolean isDone() {
            return endAt != null;
        }

        /**
         * 已耗时倒序排列
         *
         * @param o 比较对象
         * @return 结果
         */
        @Override
        public int compareTo(MonitorMeta o) {
            long now = System.currentTimeMillis();
            return Long.compare(now - o.startAt.getTime(), now - this.startAt.getTime());
        }
    }
}

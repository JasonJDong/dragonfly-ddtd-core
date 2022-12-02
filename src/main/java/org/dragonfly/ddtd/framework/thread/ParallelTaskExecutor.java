package org.dragonfly.ddtd.framework.thread;

import cn.hutool.core.util.RandomUtil;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.dragonfly.ddtd.framework.entity.Tuple2;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * 作用描述 并发任务
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2021/11/8
 **/
@Slf4j
public final class ParallelTaskExecutor {

    /**
     * 任务线程池，CacheThreadPool
     */
    private static final ThreadPoolExecutor taskExecutor = new ThreadPoolExecutor(
            0,
            Integer.MAX_VALUE,
            60L,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new ThreadFactoryBuilder().setNameFormat("ParallelTaskExecutor thread %s").build(),
            new ThreadPoolExecutor.CallerRunsPolicy()
    );
    /**
     * 线程池处理装饰
     */
    private static final ListeningExecutorService listeningExecutor = MoreExecutors.listeningDecorator(taskExecutor);

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(ParallelTaskExecutor::shutdown));
    }

    /**
     * 外部不能直接构造
     */
    private ParallelTaskExecutor() {
    }

    /**
     * 关闭线程池
     */
    public static void shutdown() {
        try {
            listeningExecutor.shutdown();
        } catch (Exception e) {
            log.error("Exception while shutting down parallel task executor.", e);
        }
    }


    /**
     * 执行单个异步任务
     *
     * @param callable 任务
     * @return 结果
     */
    public static <T> Future<T> executeAsync(Callable<T> callable,
                                             String taskName) {
        Callable<T> task = newProxy(callable, taskName);
        FutureTask<T> future = new FutureTask<>(task);
        taskExecutor.execute(future);
        return future;
    }

    /**
     * 执行单个异步任务并阻塞当前线程
     * <p>
     * 场景：任务纳入监控
     * </p>
     *
     * @param callable 任务
     * @return 结果
     */
    public static <T> T executeDone(Callable<T> callable,
                                    String taskName) {
        Callable<T> task = newProxy(callable, taskName);
        FutureTask<T> future = new FutureTask<>(task);
        taskExecutor.execute(future);
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            return null;
        }
    }

    /**
     * 执行并等待所有任务结果
     *
     * @param tasks      任务
     * @param timeoutSec 超时时间
     * @param <T>        任务数据类型
     * @return 结果，所有异常都将被忽略
     */
    public static <T> List<T> executesDone(List<Tuple2<String, ? extends Callable<T>>> tasks,
                                           int timeoutSec) {

        List<Future<T>> futures = executeBatch(tasks, timeoutSec);
        return getFutureResults(futures);
    }

    /**
     * 执行多任务
     *
     * @param tasks 任务
     * @param <T>   任务数据类型
     * @return 异步调用引用
     */
    public static <T> List<Future<T>> executeBatch(List<Tuple2<String, ? extends Callable<T>>> tasks,
                                                   int timeoutSec) {

        try {
            List<Callable<T>> taskProxies = newProxies(tasks);
            if (timeoutSec > 0) {
                return taskExecutor.invokeAll(taskProxies, timeoutSec, TimeUnit.SECONDS);
            } else {
                return taskExecutor.invokeAll(taskProxies);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * 批量代理包装
     *
     * @param tasks 任务
     * @param <T>   任务返回数据类型
     * @return 任务动态代理
     */
    private static <T> List<Callable<T>> newProxies(List<Tuple2<String, ? extends Callable<T>>> tasks) {
        return tasks.stream().map(tuple2 -> newProxy(tuple2.getT2(), tuple2.getT1())).collect(Collectors.toList());
    }

    /**
     * 包装任务代理
     *
     * @param task     任务
     * @param taskName 任务名称
     * @param <T>      任务返回数据类型
     * @return 任务动态代理
     */
    @SuppressWarnings("unchecked")
    private static <T> Callable<T> newProxy(Callable<T> task,
                                            String taskName) {
        CallableProxy<T> proxy = new CallableProxy<>(task, taskName);
        return (Callable<T>) Proxy.newProxyInstance(
                task.getClass().getClassLoader(),
                new Class[]{Callable.class},
                proxy
        );
    }

    /**
     * 获取异步线程执行结果，忽略执行异常（异常线程对应下标的返回值为null）
     *
     * @param futures 异步结果回调
     * @return 异步线程执行结果.
     */
    private static <T> List<T> getFutureResults(List<Future<T>> futures) {
        List<T> result = Lists.newArrayListWithExpectedSize(futures.size());
        for (Future<T> future : futures) {
            try {
                result.add(future.get());
            } catch (CancellationException | ExecutionException | InterruptedException e) {
                log.error("callable execute exception: {}", e.getMessage(), e);
                result.add(null);
            }
        }
        return result;
    }

    private static class CallableProxy<T> implements InvocationHandler {
        /**
         * 线程方法
         */
        private final Callable<T> callable;
        /**
         * 线程名称
         */
        private final String taskName;

        public CallableProxy(Callable<T> callable, String taskName) {
            this.callable = callable;
            this.taskName = taskName;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

            ParallelTaskMonitor.MonitorMeta monitorMeta = ParallelTaskMonitor.MonitorMeta.builder()
                    .startAt(new Date())
                    .caller(callable.getClass().getName())
                    .threadName(Thread.currentThread().getName())
                    .taskName(taskName)
                    .taskId(RandomUtil.randomString(8))
                    .build();
            ParallelTaskMonitor.add(monitorMeta);
            try {
                return method.invoke(this.callable, args);
            } catch (InvocationTargetException e) {
                // callable异常行为：已运行的call方法内部抛出异常，会被包装成ExecutionException抛出到父线程，
                // 同时当前线程interrupt置为true，当前线程响应中断抛出InterruptException
                // 待运行的call方法：如果cancel或interrupt，直接抛出相应异常到父线程；不执行call方法
                monitorMeta.setException(e);
                if (e.getCause() instanceof InterruptedException) {
                    log.warn("thread {} for {}/{} fail: interrupt by unexpect exception", Thread.currentThread().getName(),
                            this.taskName, System.currentTimeMillis() - monitorMeta.getStartAt().getTime());
                    return null;
                }
                //动态代理代码的异常会被包装为InvocationTargetException，此处抛出原始异常
                throw e.getCause();
            } finally {
                monitorMeta.setEndAt(new Date());
            }
        }
    }
}

package org.dragonfly.ddtd.framework.task;

import cn.hutool.core.util.EnumUtil;
import cn.hutool.core.util.RandomUtil;
import com.alibaba.fastjson.JSON;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.dragonfly.ddtd.curator.PathChildrenCache;
import org.dragonfly.ddtd.enums.TaskDistributionStrategy;
import org.dragonfly.ddtd.enums.TaskWorkState;
import org.dragonfly.ddtd.framework.TaskZooKeeperFactory;
import org.dragonfly.ddtd.framework.entity.ITaskContext;
import org.dragonfly.ddtd.framework.exception.DuplicatedTaskCreationException;
import org.dragonfly.ddtd.framework.inspect.ITaskInspect;
import org.dragonfly.ddtd.framework.inspect.ITaskUpDowngradeEventHandle;
import org.dragonfly.ddtd.framework.lifecycle.EmptyTaskLifeCycleHooker;
import org.dragonfly.ddtd.framework.lifecycle.ITaskLifeCycleHook;
import org.dragonfly.ddtd.framework.report.ITaskReporter;
import org.dragonfly.ddtd.framework.report.TaskProcessStage;
import org.dragonfly.ddtd.framework.thread.ParallelTaskExecutor;
import org.dragonfly.ddtd.framework.thread.ParallelTaskMonitor;
import org.dragonfly.ddtd.locker.ZooKeeperLocker;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 作用描述 基于ZooKeeper分布式任务调度
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/1/27
 **/
@Slf4j
public final class TaskDistributionDispatcher<T> implements ITaskDispatcher<T> {


    private final TaskZooKeeperFactory taskZooKeeperFactory;

    private final ITaskInspect taskInspect;

    private final ITaskReporter taskReporter;

    private final ITaskUpDowngradeEventHandle taskUpDowngradeEventHandle;

    private final ITaskLifeCycleHook taskLifeCycleHook;

    /**
     * 内部监听执行器
     */
    private InternalDistributeDispatch distributeDispatch;

    class InternalDistributeDispatch {

        private final TaskZooKeeperFactory taskZooKeeperFactory;
        /**
         * 当前子任务处理器
         */
        @Getter
        private final ITaskWorker<T> taskWorker;

        private final ITaskInspect taskInspect;

        private final ITaskReporter taskReporter;

        private final ITaskUpDowngradeEventHandle taskUpDowngradeEventHandle;

        private final ITaskLifeCycleHook taskLifeCycleHook;
        /**
         * 任务结点路径
         */
        private String nodePath;
        /**
         * 是否启动
         */
        @Getter
        private boolean started;

        public InternalDistributeDispatch(TaskZooKeeperFactory taskZooKeeperFactory,
                                          ITaskWorker<T> taskWorker,
                                          ITaskInspect taskInspect,
                                          ITaskReporter taskReporter,
                                          ITaskUpDowngradeEventHandle taskUpDowngradeEventHandle,
                                          ITaskLifeCycleHook taskLifeCycleHook) {
            this.taskZooKeeperFactory = taskZooKeeperFactory;
            this.taskWorker = taskWorker;
            this.taskInspect = taskInspect;
            this.taskReporter = taskReporter;
            this.taskUpDowngradeEventHandle = taskUpDowngradeEventHandle;
            this.taskLifeCycleHook = taskLifeCycleHook;
        }

        /**
         * 获取结点路径
         *
         * @return 结点路径
         */
        public String getNodePath() {
            Preconditions.checkArgument(StringUtils.isNotBlank(getTaskWorker().getVersion()), "动态分布式任务版本不能为空");
            Preconditions.checkArgument(StringUtils.isNotBlank(getTaskWorker().getName()), "动态分布式任务名称不能为空");
            if (StringUtils.isBlank(nodePath)) {

                this.nodePath = ZKPaths.makePath(
                        this.taskZooKeeperFactory.getRootNodePath(),
                        String.format("task_%s_%s", getTaskWorker().getName(), getTaskWorker().getVersion())
                );
            }
            return nodePath;
        }

        /**
         * 开始
         * <p>创建任务结点</p>
         * <p>
         * 主要作用是让关心本任务的调用方监控结点事件
         * </p>
         */
        public void start() throws Exception {
            if (started) {
                return;
            }
            try {
                Stat stat = this.taskZooKeeperFactory.getClient().checkExists()
                        .forPath(getNodePath());
                if (stat == null) {
                    this.taskZooKeeperFactory.getClient().create()
                            .withMode(CreateMode.PERSISTENT)
                            .forPath(getNodePath());
                } else {
                    final List<String> childrenNodes = this.taskZooKeeperFactory.getClient()
                            .getChildren()
                            .forPath(getNodePath());
                    for (String childNode : childrenNodes) {
                        final String childPath = ZKPaths.makePath(getNodePath(), childNode);
                        final List<String> grandsons = this.taskZooKeeperFactory.getClient().getChildren()
                                .forPath(childPath);
                        if (CollectionUtils.isEmpty(grandsons)) {
                            this.taskZooKeeperFactory.getClient().delete().forPath(childPath);
                        }
                    }
                }
                this.watchTaskNodeAddChildren();
            } catch (Exception e) {
                log.error("创建动态分布式任务结点失败", e);
                throw e;
            }
        }

        /**
         * 监听任务实例的创建
         */
        private void watchTaskNodeAddChildren() throws Exception {

            PathChildrenCache childrenCache = new PathChildrenCache(
                    this.taskZooKeeperFactory.getClient(),
                    this.getNodePath(),
                    true
            );

            TaskNodeChildrenAddedListener listener =
                    new TaskNodeChildrenAddedListener(
                            this.taskZooKeeperFactory,
                            this.taskWorker,
                            this.taskInspect,
                            this.taskReporter,
                            this.taskUpDowngradeEventHandle,
                            this.taskLifeCycleHook,
                            childrenCache);

            childrenCache.getListenable().addListener(listener);
            taskReporter.getSystemReporter().report(
                    String.format("启动监听：[%s]", this.getNodePath()),
                    taskWorker,
                    null,
                    TaskProcessStage.START
            );
            try {
                childrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
                this.started = true;
            } catch (Exception e) {
                taskReporter.getSystemReporter().report(
                        "启动监听异常",
                        taskWorker,
                        null,
                        TaskProcessStage.START,
                        e
                );
                throw e;
            }
        }

        /**
         * 监听任务实例创建
         */
        class TaskNodeChildrenAddedListener implements PathChildrenCacheListener {

            private final TaskZooKeeperFactory factory;

            private final ITaskWorker<T> taskWorker;

            private final ITaskInspect taskInspect;

            private final ITaskReporter taskReporter;

            private final ITaskUpDowngradeEventHandle taskUpDowngradeEventHandle;

            private final ITaskLifeCycleHook taskLifeCycleHook;

            private final PathChildrenCache childrenAddCache;

            TaskNodeChildrenAddedListener(TaskZooKeeperFactory factory,
                                          ITaskWorker<T> taskWorker,
                                          ITaskInspect taskInspect,
                                          ITaskReporter taskReporter,
                                          ITaskUpDowngradeEventHandle taskUpDowngradeEventHandle,
                                          ITaskLifeCycleHook taskLifeCycleHook,
                                          PathChildrenCache childrenAddCache) {
                this.factory = factory;
                this.taskWorker = taskWorker;
                this.taskInspect = taskInspect;
                this.taskReporter = taskReporter;
                this.taskUpDowngradeEventHandle = taskUpDowngradeEventHandle;
                this.taskLifeCycleHook = taskLifeCycleHook;
                this.childrenAddCache = childrenAddCache;
            }

            /**
             * 1、子节点增加，立即增加对子节点的删除事件进行监听
             * 2、JVM不能接受任务，循环等待
             * 3、接受任务，创建子任务节点
             *
             * @param client zooKeeper客户端
             * @param event  节点事件
             * @throws Exception 异常
             */
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {

                    ChildData data = event.getData();
                    Preconditions.checkNotNull(data, "节点数据为空，无法执行任务");
                    taskReporter.getSystemReporter().report(
                            String.format("子节点添加：%s", data.getPath()),
                            taskWorker,
                            null,
                            TaskProcessStage.EACH_START
                    );
                    TaskNode taskNode = TaskNode.builder()
                            .path(data.getPath())
                            .node(data.getStat())
                            .data(StringUtils.toEncodedString(data.getData(), StandardCharsets.UTF_8))
                            .build();
                    TaskNodeData taskNodeData = taskNode.getTaskNodeData();
                    ITaskContext taskContext = Optional.ofNullable(taskNodeData)
                            .map(ctxData -> this.taskWorker.parseContext(ctxData.getData()))
                            .orElse(null);
                    if (taskContext == null) {
                        taskReporter.getSystemReporter().report(
                                "业务数据缺失",
                                taskWorker,
                                null,
                                TaskProcessStage.EACH_START
                        );
                        return;
                    }
                    // 增加当前节点删除子节点监听
                    this.addRemoveSubNodeListen(taskNode, taskContext);

                    // 添加线程监控
                    TaskMonitorHandler monitorHandler = new TaskMonitorHandler(taskReporter, taskWorker, taskContext);
                    ParallelTaskMonitor.registerMonitorHandler(monitorHandler);

                    // 开启线程处理
                    ParallelTaskExecutor.executeAsync(() -> {

                        String subTaskNodePath = null;
                        boolean downgrade = false;
                        ITaskCacheableDataAccessor<T> cacheableDataAccessor = this.taskWorker.getTaskDataAccessor().getTaskCacheableDataAccessor();
                        ITaskPersistDataAccessor<T> persistDataAccessor = this.taskWorker.getTaskDataAccessor().getTaskPersistDataAccessor();
                        while (cacheableDataAccessor.exists(taskContext)) {
                            T taskData = null;
                            try {
                                /* 降级检测
                                 * 1、判断任务是否需要降级
                                 * 2、后续的任务清理依赖ZooKeeper，因此，需要退出循环
                                 * 3、如果是取数中间件失效，未取到数，会跳出循环，后续再判断及降级处理
                                 */
                                if (!taskInspect.getTaskUpDowngradeInspect().isDistributed(this.taskWorker, taskContext)) {
                                    taskReporter.getSystemReporter().report(
                                            "开始降级",
                                            taskWorker,
                                            taskContext,
                                            TaskProcessStage.DOWNGRADE
                                    );
                                    downgrade = true;
                                    break;
                                }

                                // 当accept为false时，不会进行等待，减少再调一次exit
                                boolean accept = this.isStrategyAccept(taskNode);
                                if (!(accept &= this.taskWorker.accept(taskContext))) {
                                    TimeUnit.MILLISECONDS.sleep(this.factory.getProperties().getPauseScanMills());
                                }
                                // 等待过程中，任务执行完毕
                                if (!accept && !cacheableDataAccessor.exists(taskContext)) {
                                    taskReporter.getSystemReporter().report(
                                            "所有任务已完成，当前JVM退出",
                                            taskWorker,
                                            taskContext,
                                            TaskProcessStage.COMPLETED
                                    );
                                    break;
                                }
                                // 这里可能接受任务
                                if (accept || this.taskWorker.accept(taskContext)) {
                                    // 创建子任务节点
                                    if (StringUtils.isBlank(subTaskNodePath)) {
                                        subTaskNodePath = this.createSubTaskNode(taskNode.getPath(), taskContext);
                                    }
                                    // 处理子任务
                                    taskData = cacheableDataAccessor.pollOne(taskContext);
                                    this.taskLifeCycleHook.onEachTaskStarted(taskWorker, taskContext, taskData);
                                    TaskWorkState taskWorkState = this.taskWorker.partitionWork(
                                            taskContext,
                                            taskData
                                    );
                                    persistDataAccessor.resolved(
                                            taskContext,
                                            taskData,
                                            taskWorkState
                                    );
                                    taskReporter.getSystemReporter().report(
                                            "子任务执行完毕，结果：" + taskWorkState.name(),
                                            taskWorker,
                                            taskContext,
                                            TaskProcessStage.EACH_COMPLETED
                                    );
                                    this.taskLifeCycleHook.onEachTaskCompleted(taskWorker, taskContext);
                                }
                            } catch (Exception e) {
                                if (e instanceof InterruptedException) {
                                    taskReporter.getSystemReporter().report(
                                            "子任务被终止",
                                            taskWorker,
                                            taskContext,
                                            TaskProcessStage.TERMINATED,
                                            e
                                    );
                                    break;
                                } else {
                                    taskReporter.getSystemReporter().report(
                                            "子任务执行异常，并继续执行",
                                            taskWorker,
                                            taskContext,
                                            TaskProcessStage.EACH_ERROR,
                                            e,
                                            taskData
                                    );
                                    this.taskLifeCycleHook.onEachTaskError(taskWorker, taskContext, e);
                                }
                            }
                        }

                        // 尝试删除任务节点
                        if (StringUtils.isNotBlank(subTaskNodePath)) {
                            try {
                                this.removeSubTaskNode(subTaskNodePath);
                                taskReporter.getSystemReporter().report(
                                        String.format("任务节点删除：[%s]", subTaskNodePath),
                                        taskWorker,
                                        taskContext,
                                        TaskProcessStage.PARTITION_COMPLETED
                                );
                            } catch (Exception e) {
                                taskReporter.getSystemReporter().report(
                                        "任务节点删除异常",
                                        taskWorker,
                                        taskContext,
                                        TaskProcessStage.PARTITION_ERROR,
                                        e,
                                        subTaskNodePath
                                );
                            }
                        }
                        // 已经跳出循环，存在未处理完成任务且有降级标记，需要降级处理
                        if (persistDataAccessor.existsUnresolved(taskContext) && downgrade) {
                            taskReporter.getSystemReporter().report(
                                    "开始降级处理",
                                    taskWorker,
                                    taskContext,
                                    TaskProcessStage.DOWNGRADE
                            );
                            this.taskUpDowngradeEventHandle.emitDowngrade(this.taskWorker, taskContext);
                        }
                        return null;
                    }, this.taskWorker.getName());
                }
            }

            /**
             * 增加删除子节点监听
             *
             * @param taskNode 节点
             * @throws Exception 监听异常
             */
            private void addRemoveSubNodeListen(TaskNode taskNode, ITaskContext context) throws Exception {

                try {
                    // 子节点任务删除监听
                    this.watchSubTaskNodeRemoved(taskNode.getPath());

                    taskReporter.getSystemReporter().report(
                            "开始监听任务实例的子任务的删除",
                            taskWorker,
                            context,
                            TaskProcessStage.EACH_START
                    );
                } catch (Exception e) {
                    taskReporter.getSystemReporter().report(
                            "任务实例的子节点任务删除监听异常",
                            taskWorker,
                            null,
                            TaskProcessStage.EACH_START,
                            e,
                            context
                    );
                    log.error("子节点任务删除监听异常", e);
                    throw e;
                }
            }

            /**
             * 是否策略接受
             *
             * @param taskNode 任务节点
             * @return 是否策略接受
             */
            private boolean isStrategyAccept(TaskNode taskNode) {

                TaskNodeData taskNodeData = taskNode.getTaskNodeData();
                if (taskNodeData == null) {
                    return true;
                }
                TaskDistributionStrategy strategy =
                        EnumUtil.fromString(TaskDistributionStrategy.class, taskNodeData.getDistributionStrategy());

                switch (strategy) {
                    case FIXED:
                        try {
                            Stat stat = this.factory.getClient().checkExists()
                                    .forPath(taskNode.getPath());
                            if (stat != null && taskNodeData.getFixedCount() != null) {
                                return stat.getNumChildren() <= taskNodeData.getFixedCount();
                            }
                        } catch (Exception e) {
                            log.error("判断分布式策略时失败", e);
                            return true;
                        }
                        break;
                    case DYNAMIC:
                        return this.taskInspect.getTaskPerformanceInspect().isNormal();
                    default:
                        break;
                }
                return true;
            }

            /**
             * 监听任务实例的子任务的删除
             *
             * @param taskNodePath 任务实例路径
             */
            private void watchSubTaskNodeRemoved(String taskNodePath) throws Exception {

                PathChildrenCache childrenCache = new PathChildrenCache(
                        this.factory.getClient(),
                        taskNodePath,
                        true
                );

                SubTaskNodeRemoveListener listener =
                        new SubTaskNodeRemoveListener(
                                this.factory,
                                this.taskWorker,
                                this.taskInspect,
                                this.taskReporter,
                                taskLifeCycleHook,
                                taskNodePath,
                                this.childrenAddCache,
                                childrenCache
                        );
                childrenCache.getListenable().addListener(listener);
                childrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
            }

            /**
             * 创建子任务节点
             *
             * @param taskNodePath 父节点路径
             * @param context      任务上下文
             * @throws Exception 创建异常
             */
            private String createSubTaskNode(String taskNodePath, ITaskContext context) throws Exception {

                String childPath = ZKPaths.makePath(taskNodePath, RandomUtil.randomString(4));

                // 在节点上添加任务上下文数据
                String ctxContent = JSON.toJSONString(context);
                TaskNodeData taskNodeData = new TaskNodeData();
                taskNodeData.setDistributionStrategy(context.getStrategy().name());
                taskNodeData.setFixedCount(context.getFixedCount());
                taskNodeData.setData(ctxContent);

                return this.factory.getClient().create()
                        .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                        .forPath(childPath, JSON.toJSONBytes(taskNodeData));
            }

            /**
             * 删除子任务节点
             *
             * @param taskNodePath 子任务节点路径
             * @throws Exception 删除异常
             */
            private void removeSubTaskNode(String taskNodePath) throws Exception {
                Stat stat = this.factory.getClient().checkExists()
                        .forPath(taskNodePath);
                if (stat != null) {
                    this.factory.getClient().delete()
                            .forPath(taskNodePath);
                }
            }
        }

        /**
         * 子节点任务删除（完成）监听
         */
        class SubTaskNodeRemoveListener implements PathChildrenCacheListener {

            public static final String REMOVE_LOCKS = "task_node_remove_locks";

            private final TaskZooKeeperFactory factory;

            private final ITaskWorker<T> taskWorker;

            private final ITaskInspect taskInspect;

            private final ITaskReporter taskReporter;

            private final ITaskLifeCycleHook taskLifeCycleHook;

            private final String parentPath;

            private final PathChildrenCache childrenAddCache;

            private final PathChildrenCache childrenRemoveCache;

            /**
             * 每个worker在节点删除消息来时，创建1个处理
             * <p>
             * 弱引用worker，避免内存泄漏
             * </p>
             */
            private final Map<String, InternalCleanupWorkHandler> cleanupHandlers = Maps.newConcurrentMap();

            SubTaskNodeRemoveListener(TaskZooKeeperFactory factory,
                                      ITaskWorker<T> taskWorker,
                                      ITaskInspect taskInspect,
                                      ITaskReporter taskReporter,
                                      ITaskLifeCycleHook taskLifeCycleHook,
                                      String parentPath,
                                      PathChildrenCache childrenAddCache,
                                      PathChildrenCache childrenRemoveCache) {
                this.factory = factory;
                this.taskInspect = taskInspect;
                this.taskLifeCycleHook = taskLifeCycleHook;
                this.parentPath = parentPath;
                this.taskWorker = taskWorker;
                this.taskReporter = taskReporter;
                this.childrenAddCache = childrenAddCache;
                this.childrenRemoveCache = childrenRemoveCache;
                Preconditions.checkNotNull(factory, "动态分布式任务工厂不能为空");
                Preconditions.checkNotNull(getParentStat(), parentPath + "不存在，无法监听其删除事件");
                Preconditions.checkNotNull(taskWorker, "taskWorker不能为空");
                Preconditions.checkNotNull(taskReporter, "taskReporter不能为空");
                Preconditions.checkNotNull(childrenAddCache, "childrenAddCache不能为空");
                Preconditions.checkNotNull(childrenRemoveCache, "childrenRemoveCache不能为空");
            }

            private Stat getParentStat() {
                try {
                    return this.factory.getClient().checkExists()
                            .forPath(parentPath);
                } catch (Exception e) {
                    log.warn("检查{}异常", parentPath, e);
                }
                return null;
            }

            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {

                if (event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
                    ChildData data = event.getData();
                    Preconditions.checkNotNull(data, "节点数据为空，无法执行任务");
                    taskReporter.getSystemReporter().report(
                            String.format("子节点删除：[%s]", data.getPath()),
                            taskWorker,
                            null,
                            TaskProcessStage.COMPLETED
                    );
                    TaskNode taskNode = TaskNode.builder()
                            .path(data.getPath())
                            .node(data.getStat())
                            .data(StringUtils.toEncodedString(data.getData(), StandardCharsets.UTF_8))
                            .build();
                    TaskNodeData taskNodeData = taskNode.getTaskNodeData();
                    ITaskContext taskContext = Optional.ofNullable(taskNodeData)
                            .map(ctxData -> this.taskWorker.parseContext(ctxData.getData()))
                            .orElse(null);
                    if (taskContext == null) {
                        taskReporter.getSystemReporter().report(
                                "删除业务数据时任务上下文缺失",
                                taskWorker,
                                null,
                                TaskProcessStage.COMPLETED
                        );
                        return;
                    }
                    if (!this.taskInspect.getTaskUpDowngradeInspect().isDistributed(this.taskWorker, taskContext)) {
                        // 任务节点被删除，但当前为非分布式环境，处理权交由降级处理，当前忽略
                        return;
                    }
                    // 添加线程监控
                    TaskMonitorHandler monitorHandler = new TaskMonitorHandler(taskReporter, taskWorker, taskContext);
                    ParallelTaskMonitor.registerMonitorHandler(monitorHandler);

                    ParallelTaskExecutor.executeAsync(() -> {

                        /*
                         * 当前REMOVE事件触发，获取父节点子节点个数
                         * - 如果个数【大于】0，则创建清理handler，并缓存持有，并创建栅栏等待
                         * - 多次进入【大于】0，需要检查是否栅栏还存在，不存在，说明其他JVM
                         *   取父节点时发现子节点为0，并进行了通知；如果handler已经创建，也会结束等待；如果未创建，则忽略
                         * - 如果个数【等于】0，则删除栅栏，让所有线程去竞争锁
                         * - 如果未取到父节点，那么说明无需处理，清理handler后退出
                         */
                        InternalCleanupWorkHandler handler = this.cleanupHandlers.get(taskContext.getTaskId());

                        Stat parentStat = getParentStat();
                        if (parentStat == null) {
                            // 如果父节点不存在，则不需要再创建handler
                            if (handler != null) {
                                if (!this.isDistributedBarrierNotExists(handler)) {
                                    handler.notifyHandle();
                                }
                                this.cleanupHandlers.remove(taskContext.getTaskId());
                            }
                        } else if (parentStat.getNumChildren() > 0) {
                            // 多次进入的判断依据，handler是否已经创建
                            if (handler == null) {
                                // 第一次创建
                                handler = new InternalCleanupWorkHandler(
                                        this.factory,
                                        this.taskWorker,
                                        this.taskReporter,
                                        this.taskLifeCycleHook,
                                        this.parentPath,
                                        this.childrenAddCache,
                                        this.childrenRemoveCache
                                );
                                handler.waitingThenCleanup(taskContext);
                                this.cleanupHandlers.put(taskContext.getTaskId(), handler);
                            } else {
                                // 判断是否已经删除栅栏
                                if (this.isDistributedBarrierNotExists(handler)) {
                                    // 栅栏已经不存在了，当前handler也进入了处理
                                    // 删除缓存即可
                                    this.cleanupHandlers.remove(taskContext.getTaskId());
                                }
                            }
                        } else {
                            // 子节点数量为0，则可以通知处理了
                            if (handler != null) {
                                handler.notifyHandle();
                                this.cleanupHandlers.remove(taskContext.getTaskId());
                            } else {

                                // 当等待通知到来时，仍然没有创建handler，需要创建
                                handler = new InternalCleanupWorkHandler(
                                        this.factory,
                                        this.taskWorker,
                                        this.taskReporter,
                                        this.taskLifeCycleHook,
                                        this.parentPath,
                                        this.childrenAddCache,
                                        this.childrenRemoveCache
                                );
                                // make sure enter the process
                                handler.waitingThenCleanup(taskContext);
                                handler.notifyHandle();
                            }
                        }
                        return null;
                    }, "CHILD_REMOVED");
                }
            }

            /**
             * 是否栅栏不存在
             *
             * @param handler 处理器
             * @return true - 不存在
             */
            private boolean isDistributedBarrierNotExists(InternalCleanupWorkHandler handler) {
                try {
                    return this.factory.getClient().checkExists()
                            .forPath(handler.getDistributedBarrierPath()) == null;
                } catch (Exception e) {
                    return true;
                }
            }

            /**
             * 具体执行清理工作
             * <p>
             * 通过分布式栅栏等待所有删除消息到达<br/>
             * 再获取分布式锁处理清理工作
             * </p>
             */
            class InternalCleanupWorkHandler {

                private final TaskZooKeeperFactory factory;

                private final ITaskWorker<T> taskWorker;

                private final ITaskReporter taskReporter;

                private final ITaskLifeCycleHook taskLifeCycleHook;

                private final String parentPath;

                @Getter
                private final String distributedBarrierPath;

                private final PathChildrenCache childrenAddCache;

                private final PathChildrenCache childrenRemoveCache;

                InternalCleanupWorkHandler(TaskZooKeeperFactory factory,
                                           ITaskWorker<T> taskWorker,
                                           ITaskReporter taskReporter,
                                           ITaskLifeCycleHook taskLifeCycleHook,
                                           String parentPath,
                                           PathChildrenCache childrenAddCache,
                                           PathChildrenCache childrenRemoveCache) {
                    this.factory = factory;
                    this.taskWorker = taskWorker;
                    this.taskReporter = taskReporter;
                    this.taskLifeCycleHook = taskLifeCycleHook;
                    this.parentPath = parentPath;
                    this.childrenAddCache = childrenAddCache;
                    this.childrenRemoveCache = childrenRemoveCache;
                    this.distributedBarrierPath = this.makeDistributedBarrierPath();
                }

                /**
                 * 获取分布式栅栏路径
                 *
                 * @return 分布式栅栏路径
                 */
                private String makeDistributedBarrierPath() {
                    return ZKPaths.makePath(this.factory.getProperties().getNamespace(),
                            "clean_work",
                            "barrier",
                            this.taskWorker.getName(),
                            this.taskWorker.getVersion(),
                            this.parentPath);
                }

                /**
                 * 等待所有JVM完成处理后，进行清理工作
                 *
                 * @param taskContext 任务上下文
                 * @throws Exception 异常
                 */
                public void waitingThenCleanup(ITaskContext taskContext) throws Exception {

                    // 添加线程监控
                    TaskMonitorHandler monitorHandler = new TaskMonitorHandler(taskReporter, taskWorker, taskContext);
                    ParallelTaskMonitor.registerMonitorHandler(monitorHandler);
                    // 开启线程处理
                    ParallelTaskExecutor.executeAsync(() -> {
                        // The barrier would block this thread, and the current thread must own this object's monitor.
                        // so we can only defined in the code block.
                        DistributedBarrier barrier = new DistributedBarrier(factory.getClient(), this.distributedBarrierPath);
                        barrier.setBarrier();
                        /*
                         * 等待所有节点被删除完毕再统一竞争锁
                         * 否则可能导致ZK连接断开后，去获取锁导致异常
                         * 必须设置超时时间，防止永久等待
                         * 超时后发现有结点仍在执行，那么继续等待
                         */
                        try {
                            taskReporter.getSystemReporter().report(
                                    "开始等待其他子任务处理完成",
                                    taskWorker,
                                    taskContext,
                                    TaskProcessStage.COMPLETED
                            );
                            final long waitingTime = 30L;
                            while (!barrier.waitOnBarrier(waitingTime, TimeUnit.SECONDS)) {
                                taskReporter.getSystemReporter().report(
                                        "等待处理清理任务时等待超时",
                                        taskWorker,
                                        taskContext,
                                        TaskProcessStage.COMPLETED
                                );
                                Stat parentStat = getParentStat();
                                if (parentStat != null) {
                                    if (parentStat.getNumChildren() == 0) {
                                        break;
                                    }
                                    taskReporter.getSystemReporter().report(
                                            "等待处理清理任务时超时后发现有其他JVM仍然在执行任务，继续等待",
                                            taskWorker,
                                            taskContext,
                                            TaskProcessStage.COMPLETED
                                    );
                                } else {
                                    // 已经被删除，无需继续执行
                                    taskReporter.getSystemReporter().report(
                                            "等待处理清理任务时超时后发现任务节点已删除，无需继续执行",
                                            taskWorker,
                                            taskContext,
                                            TaskProcessStage.COMPLETED
                                    );
                                    return null;
                                }
                            }
                        } catch (Exception e) {
                            taskReporter.getSystemReporter().report(
                                    "待处理清理任务时异常",
                                    taskWorker,
                                    taskContext,
                                    TaskProcessStage.COMPLETED,
                                    e
                            );
                            return null;
                        }
                        // Clear the listen cache node, avoid emit the another add child event.
                        this.childrenAddCache.clear(this.parentPath);
                        // Remove listener now should close.
                        this.childrenRemoveCache.close();

                        /* 当子节点删除时判断
                         * 是否当前父节点没有子节点，没有则表明任务都完成，当前父节点需要删除
                         * 分布式锁环境下进行
                         */
                        String taskLocks = ZKPaths.makePath(REMOVE_LOCKS, taskContext.getTaskId());
                        try (ZooKeeperLocker ignored = new ZooKeeperLocker(this.factory, taskLocks, 200L, TimeUnit.MILLISECONDS)) {
                            /* 子节点删除，多JVM都会收到通知
                             * 在分布式环境下，即使先后获得锁，进入当前代码块
                             * 但由于节点已经删除，不会造成多次清理方法调用
                             */
                            Stat parentStat = getParentStat();
                            if (parentStat != null) {
                                // Delete the parent node from here.
                                this.factory.getClient().delete()
                                        .forPath(this.parentPath);
                                taskReporter.getSystemReporter().report(
                                        String.format("已删除删除总任务节点：[%s]", this.parentPath),
                                        taskWorker,
                                        taskContext,
                                        TaskProcessStage.COMPLETED
                                );
                                // 此处代码顺序不能变，需要先删除节点
                                // 清理工作可能产生异常，导致节点无法删除
                                TaskWorkState taskWorkState = this.taskWorker.cleanupWork(taskContext);
                                taskReporter.getSystemReporter().report(
                                        "清理工作结果",
                                        taskWorker,
                                        taskContext,
                                        TaskProcessStage.COMPLETED,
                                        taskWorkState
                                );
                                this.taskLifeCycleHook.onTaskCompleted(this.taskWorker, taskContext);
                            }
                        } catch (Exception e) {
                            if (!(e instanceof TimeoutException)) {
                                taskReporter.getSystemReporter().report(
                                        "删除总任务节点异常",
                                        taskWorker,
                                        taskContext,
                                        TaskProcessStage.COMPLETED_ERROR,
                                        e
                                );
                                log.error("删除总任务节点异常", e);
                            }
                        }
                        return null;
                    }, taskWorker.getName());

                    this.notifyHandle();
                }

                /**
                 * 删除栅栏，通知所有线程开始处理
                 */
                public void notifyHandle() {
                    try {
                        Stat parentStat = getParentStat();
                        if (parentStat == null || parentStat.getNumChildren() == 0) {
                            if (!isDistributedBarrierNotExists(this)) {
                                this.removeBarrier();
                            }
                        }
                    } catch (Exception e) {
                        log.error("尝试移除栅栏以执行清理时异常", e);
                    }
                }

                /**
                 * Remove the barrier
                 *
                 * @throws Exception any remove exception
                 */
                private void removeBarrier() throws Exception {
                    new DistributedBarrier(factory.getClient(), this.distributedBarrierPath)
                            .removeBarrier();
                }
            }
        }
    }

    public TaskDistributionDispatcher(TaskZooKeeperFactory taskZooKeeperFactory,
                                      ITaskInspect taskInspect,
                                      ITaskReporter taskReporter,
                                      ITaskUpDowngradeEventHandle taskUpDowngradeEventHandle) {
        this(taskZooKeeperFactory, taskInspect, taskReporter, taskUpDowngradeEventHandle, new EmptyTaskLifeCycleHooker());
    }

    public TaskDistributionDispatcher(TaskZooKeeperFactory taskZooKeeperFactory,
                                      ITaskInspect taskInspect,
                                      ITaskReporter taskReporter,
                                      ITaskUpDowngradeEventHandle taskUpDowngradeEventHandle,
                                      ITaskLifeCycleHook taskLifeCycleHook) {
        Preconditions.checkNotNull(taskZooKeeperFactory, "zooKeeperFactory不能为空");
        Preconditions.checkNotNull(taskInspect, "taskInspect不能为空");
        Preconditions.checkNotNull(taskReporter, "taskReporter不能为空");
        Preconditions.checkNotNull(taskUpDowngradeEventHandle, "taskUpDowngradeEventHandle不能为空");
        Preconditions.checkNotNull(taskLifeCycleHook, "taskLifeCycleHook不能为空");
        this.taskZooKeeperFactory = taskZooKeeperFactory;
        this.taskInspect = taskInspect;
        this.taskReporter = taskReporter;
        this.taskUpDowngradeEventHandle = taskUpDowngradeEventHandle;
        this.taskLifeCycleHook = taskLifeCycleHook;
    }

    /**
     * 当前JVM启动监听
     *
     * @param taskWorker 任务处理器
     */
    public void startListening(ITaskWorker<T> taskWorker) throws Exception {

        if (this.distributeDispatch == null) {

            // 启动监听
            this.distributeDispatch = new InternalDistributeDispatch(
                    this.taskZooKeeperFactory,
                    taskWorker,
                    this.taskInspect,
                    this.taskReporter,
                    taskUpDowngradeEventHandle, taskLifeCycleHook);
            try {
                distributeDispatch.start();
            } catch (Exception e) {
                taskReporter.getSystemReporter().report(
                        "监听任务失败",
                        taskWorker,
                        null,
                        TaskProcessStage.PREPARE,
                        e
                );
                throw e;
            }
        }
    }

    @Override
    public void startDispatch(ITaskWorker<T> taskWorker, ITaskContext context, Collection<T> data) throws Exception {

        if (CollectionUtils.isEmpty(data)) {
            throw new IllegalArgumentException("The task data must not be null.");
        }

        // 保存数据
        try {
            taskWorker.getTaskDataAccessor().getTaskPersistDataAccessor().persist(context, data);
            // 存储总任务数据
            taskWorker.getTaskDataAccessor().getTaskCacheableDataAccessor().cacheAllData(context, data);
            taskReporter.getSystemReporter().report(
                    "已经保存任务数据",
                    taskWorker,
                    context,
                    TaskProcessStage.PREPARE
            );
        } catch (Exception e) {
            taskReporter.getSystemReporter().report(
                    "任务数据无法保存，创建任务失败",
                    taskWorker,
                    context,
                    TaskProcessStage.PREPARE,
                    e
            );
            throw e;
        }

        if (this.distributeDispatch == null) {
            // 先启动监听
            this.distributeDispatch = new InternalDistributeDispatch(
                    this.taskZooKeeperFactory,
                    taskWorker,
                    this.taskInspect,
                    this.taskReporter,
                    this.taskUpDowngradeEventHandle, taskLifeCycleHook);
        }
        try {
            distributeDispatch.start();
            // 创建任务实例节点
            this.createTaskNodeInstance(taskWorker, context, distributeDispatch);
            this.taskLifeCycleHook.onTaskStarted(taskWorker, context);
        } catch (Exception e) {
            taskReporter.getSystemReporter().report(
                    "创建任务失败",
                    taskWorker,
                    context,
                    TaskProcessStage.START,
                    e
            );
            taskWorker.getTaskDataAccessor().getTaskPersistDataAccessor().clear(context);
            taskWorker.getTaskDataAccessor().getTaskCacheableDataAccessor().clear(context);
        }
    }

    /**
     * 创建任务实例节点
     * <p>
     * 任务节点名称为task_{worker.name}_{worker.version}_{context.taskId}
     * </p>
     *
     * @param taskWorker                 任务处理器
     * @param context                    任务上下文
     * @param internalDistributeDispatch 调度器
     */
    private void createTaskNodeInstance(ITaskWorker<T> taskWorker,
                                        ITaskContext context,
                                        InternalDistributeDispatch internalDistributeDispatch) throws Exception {

        try {
            String taskInsName = String.format("task_%s_%s_%s", taskWorker.getName(), taskWorker.getVersion(), context.getTaskId());
            String path = ZKPaths.makePath(internalDistributeDispatch.getNodePath(), taskInsName);
            Stat stat = this.taskZooKeeperFactory.getClient().checkExists().forPath(path);
            if (stat != null) {
                // 判断是否当前已存在的节点是否存在子节点，不存在，需要删除后再添加
                if (stat.getNumChildren() == 0) {
                    log.info("createTaskNodeInstance：{}，stat != null，stat.getNumChildren() == 0", path);
                    this.taskZooKeeperFactory.getClient()
                            .delete().forPath(path);
                } else {
                    // 有相同任务Id任务正在执行
                    throw new DuplicatedTaskCreationException(taskWorker, context);
                }
            }
            // 在节点上添加任务上下文数据
            String ctxContent = JSON.toJSONString(context);
            TaskNodeData taskNodeData = new TaskNodeData();
            taskNodeData.setDistributionStrategy(context.getStrategy().name());
            taskNodeData.setFixedCount(context.getFixedCount());
            taskNodeData.setData(ctxContent);

            this.taskZooKeeperFactory.getClient()
                    .create()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path, JSON.toJSONBytes(taskNodeData));
        } catch (Exception e) {
            taskReporter.getSystemReporter().report(
                    "创建任务实例异常",
                    taskWorker,
                    context,
                    TaskProcessStage.START,
                    e
            );
            throw e;
        }
    }

    @Override
    public void terminate(ITaskWorker<T> taskWorker, ITaskContext context) {
        // 中止任务将通过清理任务数据实现
        taskReporter.getSystemReporter().report(
                "主动中止任务",
                taskWorker,
                context,
                TaskProcessStage.TERMINATED
        );
        taskWorker.getTaskDataAccessor().getTaskPersistDataAccessor().terminate(context);
        taskWorker.getTaskDataAccessor().getTaskCacheableDataAccessor().clear(context);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
}

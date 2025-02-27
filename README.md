# ddtd

主动式动态分布式任务调度（Dynamic-Distributed-Task-Dispatcher），包含动态组件构建的任务工作台，当然也可以仅使用调度，构建自己的前端
> 不同于Elastic-Job，xxx-Job等被动式任务调度（常见于通过cron配置后进行周期性轮询），用户可以在发起调用后，立即进行分布式处理
## 设计图
* 整体流程图
![alt 流程图](流程图.jpg)
* 工作原理说明
  - 框架启动后，将根据任务名称和版本号创建ZooKeeper永久节点
  > 每个JVM实例都会尝试去创建任务节点，此为任务工作空间，所有JVM会监听此任务空间下是否有任务执行节点创建
  > 带任务数据的JVM发起调用，会在任务工作空间创建任务执行节点，监听的JVM将会收到子节点创建通知
  - 任务数据按照业务逻辑拆分保存到数据库/缓存中，待各JVM获取
  - JVM收到子节点创建通知后，会监听任务执行节点子节点删除通知，如果任务执行节点下子节点为0时，加锁删除任务执行节点
  - 然后JVM同时会从数据库/缓存获取拆分后的子任务数据，开启线程循环进行业务逻辑处理
  - 无子任务数据后，删除子节点，所有子节点删除完毕后，全任务流程结束

* 升降级机制设计
    - 当分布式调度所需组件失效，为了保证业务依然能正常进行，需要对分布式调度流程进行降级，变成单机任务执行。
    - 当降级执行任务，可根据策略配置进行自/手动升级，重新对剩余任务进行分布式执行
![alt 升降级](升降级设计.jpg)

## 作用
ddtd主要作用是根据大任务拆分为小任务后，调度多个JVM实例进行小任务处理，并提供任务执行监控、任务恢复、任务中止、任务降/升级等，旨在提高大任务
的执行效率，且能可视化监控整个任务执行中的情况，提供API能有效的介入任务处理。

## 使用场景
* 大任务，无法使用或使用单JVM处理性能低下
* 关键任务，需要详细监控和介入
* 资源紧张，需要随时动态调整JVM并发数量

## 用法
* 引入
```xml
<dependency>
    <groupId>org.dragonfly</groupId>
    <artifactId>ddtd</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.execution.scheduler;

import com.facebook.airlift.concurrent.SetThreadName;
import com.facebook.airlift.stats.TimeStat;
import com.facebook.presto.Session;
import com.facebook.presto.execution.BasicStageExecutionStats;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStateMachine;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.RemoteTaskFactory;
import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.execution.StageExecutionInfo;
import com.facebook.presto.execution.StageExecutionState;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SplitSourceFactory;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.facebook.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static com.facebook.airlift.concurrent.MoreFutures.whenAnyComplete;
import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.presto.SystemSessionProperties.getMaxConcurrentMaterializations;
import static com.facebook.presto.SystemSessionProperties.getMaxStageRetries;
import static com.facebook.presto.execution.BasicStageExecutionStats.aggregateBasicStageStats;
import static com.facebook.presto.execution.SqlStageExecution.RECOVERABLE_ERROR_CODES;
import static com.facebook.presto.execution.StageExecutionInfo.unscheduledExecutionInfo;
import static com.facebook.presto.execution.StageExecutionState.CANCELED;
import static com.facebook.presto.execution.StageExecutionState.FAILED;
import static com.facebook.presto.execution.StageExecutionState.FINISHED;
import static com.facebook.presto.execution.StageExecutionState.RUNNING;
import static com.facebook.presto.execution.StageExecutionState.SCHEDULED;
import static com.facebook.presto.execution.buffer.OutputBuffers.BROADCAST_PARTITION_ID;
import static com.facebook.presto.execution.buffer.OutputBuffers.createDiscardingOutputBuffers;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.execution.scheduler.StreamingPlanSection.extractStreamingSections;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.sql.planner.PlanFragmenter.ROOT_FRAGMENT_ID;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Streams.stream;
import static com.google.common.graph.Traverser.forTree;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class SqlQueryScheduler
        implements SqlQuerySchedulerInterface
{
    private final LocationFactory locationFactory;
    private final ExecutionPolicy executionPolicy;
    private final ExecutorService executor;
    private final SplitSchedulerStats schedulerStats;
    private final SectionExecutionFactory sectionExecutionFactory;
    private final RemoteTaskFactory remoteTaskFactory;
    private final SplitSourceFactory splitSourceFactory;
    private final InternalNodeManager nodeManager;

    private final Session session;
    private final QueryStateMachine queryStateMachine;
    private final SubPlan plan;
    private final StreamingPlanSection sectionedPlan;
    private final boolean summarizeTaskInfo;
    private final int maxConcurrentMaterializations;
    private final int maxStageRetries;

    private final Map<StageId, List<SectionExecution>> sectionExecutions = new ConcurrentHashMap<>();
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean scheduling = new AtomicBoolean();
    private final AtomicInteger retriedSections = new AtomicInteger();

    public static SqlQueryScheduler createSqlQueryScheduler(
            LocationFactory locationFactory,
            ExecutionPolicy executionPolicy,
            ExecutorService executor,
            SplitSchedulerStats schedulerStats,
            SectionExecutionFactory sectionExecutionFactory,
            RemoteTaskFactory remoteTaskFactory,
            SplitSourceFactory splitSourceFactory,
            InternalNodeManager nodeManager,
            Session session,
            QueryStateMachine queryStateMachine,
            SubPlan plan,
            boolean summarizeTaskInfo)
    {
        SqlQueryScheduler sqlQueryScheduler = new SqlQueryScheduler(
                locationFactory,
                executionPolicy,
                executor,
                schedulerStats,
                sectionExecutionFactory,
                remoteTaskFactory,
                splitSourceFactory,
                nodeManager,
                session,
                queryStateMachine,
                plan,
                summarizeTaskInfo);
        sqlQueryScheduler.initialize();
        return sqlQueryScheduler;
    }

    private SqlQueryScheduler(
            LocationFactory locationFactory,
            ExecutionPolicy executionPolicy,
            ExecutorService executor,
            SplitSchedulerStats schedulerStats,
            SectionExecutionFactory sectionExecutionFactory,
            RemoteTaskFactory remoteTaskFactory,
            SplitSourceFactory splitSourceFactory,
            InternalNodeManager nodeManager,
            Session session,
            QueryStateMachine queryStateMachine,
            SubPlan plan,
            boolean summarizeTaskInfo)
    {
        this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
        this.executionPolicy = requireNonNull(executionPolicy, "schedulerPolicyFactory is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
        this.sectionExecutionFactory = requireNonNull(sectionExecutionFactory, "sectionExecutionFactory is null");
        this.remoteTaskFactory = requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
        this.splitSourceFactory = requireNonNull(splitSourceFactory, "splitSourceFactory is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.session = requireNonNull(session, "session is null");
        this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
        this.plan = requireNonNull(plan, "plan is null");
        this.sectionedPlan = extractStreamingSections(plan);
        this.summarizeTaskInfo = summarizeTaskInfo;
        this.maxConcurrentMaterializations = getMaxConcurrentMaterializations(session);
        this.maxStageRetries = getMaxStageRetries(session);
    }

    // this is a separate method to ensure that the `this` reference is not leaked during construction
    private void initialize()
    {
        // when query is done or any time a stage completes, attempt to transition query to "final query info ready"
        queryStateMachine.addStateChangeListener(newState -> {
            if (newState.isDone()) {
                queryStateMachine.updateQueryInfo(Optional.of(getStageInfo()));
            }
        });
    }

    @Override
    private static void updateQueryOutputLocations(QueryStateMachine queryStateMachine, OutputBufferId rootBufferId, Set<RemoteTask> tasks, boolean noMoreExchangeLocations)
    {
        Map<URI, TaskId> bufferLocations = tasks.stream()
                .collect(toImmutableMap(
                        task -> getBufferLocation(task, rootBufferId),
                        RemoteTask::getTaskId));
        queryStateMachine.updateOutputLocations(bufferLocations, noMoreExchangeLocations);
    }

    private static URI getBufferLocation(RemoteTask remoteTask, OutputBufferId rootBufferId)
    {
        URI location = remoteTask.getTaskStatus().getSelf();
        return uriBuilderFrom(location).appendPath("results").appendPath(rootBufferId.toString()).build();
    }

    /**
     * returns a List of SqlStageExecutionInfos in a postorder representation of the tree
     */
    private List<StageExecutionAndScheduler> createStageExecutions(
            ExchangeLocationsConsumer locationsConsumer,
            StreamingPlanSection section,
            Optional<int[]> bucketToPartition,
            Metadata metadata,
            OutputBuffers outputBuffers,
            NodeScheduler nodeScheduler,
            RemoteTaskFactory remoteTaskFactory,
            SplitSourceFactory splitSourceFactory,
            Session session,
            int splitBatchSize,
            NodePartitioningManager nodePartitioningManager,
            ExecutorService queryExecutor,
            ScheduledExecutorService schedulerExecutor,
            FailureDetector failureDetector,
            NodeTaskMap nodeTaskMap)
    {
        ImmutableList.Builder<StageExecutionAndScheduler> stages = ImmutableList.builder();

        for (StreamingPlanSection childSection : section.getChildren()) {
            stages.addAll(createStageExecutions(
                    discardingLocationConsumer(),
                    childSection,
                    Optional.empty(),
                    metadata,
                    createDiscardingOutputBuffers(),
                    nodeScheduler,
                    remoteTaskFactory,
                    splitSourceFactory,
                    session,
                    splitBatchSize,
                    nodePartitioningManager,
                    queryExecutor,
                    schedulerExecutor,
                    failureDetector,
                    nodeTaskMap));
        }

        // Only fetch a distribution once per section to ensure all stages see the same machine assignments
        Map<PartitioningHandle, NodePartitionMap> partitioningCache = new HashMap<>();
        TableWriteInfo tableWriteInfo = createTableWriteInfo(section.getPlan(), metadata, session);
        List<StageExecutionAndScheduler> sectionStages = createStreamingLinkedStageExecutions(
                locationsConsumer,
                section.getPlan().withBucketToPartition(bucketToPartition),
                nodeScheduler,
                remoteTaskFactory,
                splitSourceFactory,
                session,
                splitBatchSize,
                partitioningHandle -> partitioningCache.computeIfAbsent(partitioningHandle, handle -> nodePartitioningManager.getNodePartitioningMap(session, handle)),
                nodePartitioningManager,
                queryExecutor,
                schedulerExecutor,
                failureDetector,
                nodeTaskMap,
                tableWriteInfo,
                Optional.empty());
        Iterables.getLast(sectionStages)
                .getStageExecution()
                .setOutputBuffers(outputBuffers);
        stages.addAll(sectionStages);

        return stages.build();
    }

    /**
     * returns a List of SqlStageExecutionInfos in a postorder representation of the tree
     */
    private List<StageExecutionAndScheduler> createStreamingLinkedStageExecutions(
            ExchangeLocationsConsumer parent,
            StreamingSubPlan plan,
            NodeScheduler nodeScheduler,
            RemoteTaskFactory remoteTaskFactory,
            SplitSourceFactory splitSourceFactory,
            Session session,
            int splitBatchSize,
            Function<PartitioningHandle, NodePartitionMap> partitioningCache,
            NodePartitioningManager nodePartitioningManager,
            ExecutorService queryExecutor,
            ScheduledExecutorService schedulerExecutor,
            FailureDetector failureDetector,
            NodeTaskMap nodeTaskMap,
            TableWriteInfo tableWriteInfo,
            Optional<SqlStageExecution> parentStageExecution)
    {
        ImmutableList.Builder<StageExecutionAndScheduler> stageExecutionInfos = ImmutableList.builder();

        PlanFragmentId fragmentId = plan.getFragment().getId();
        StageId stageId = getStageId(fragmentId);
        SqlStageExecution stageExecution = createSqlStageExecution(
                new StageExecutionId(stageId, 0),
                plan.getFragment(),
                remoteTaskFactory,
                session,
                summarizeTaskInfo,
                nodeTaskMap,
                queryExecutor,
                failureDetector,
                schedulerStats,
                tableWriteInfo);

        PartitioningHandle partitioningHandle = plan.getFragment().getPartitioning();
        Map<PlanNodeId, SplitSource> splitSources = splitSourceFactory.createSplitSources(plan.getFragment(), session, tableWriteInfo);
        List<RemoteSourceNode> remoteSourceNodes = plan.getFragment().getRemoteSourceNodes();
        Optional<int[]> bucketToPartition = getBucketToPartition(partitioningHandle, partitioningCache, splitSources, remoteSourceNodes);

        // create child stages
        ImmutableSet.Builder<SqlStageExecution> childStagesBuilder = ImmutableSet.builder();
        for (StreamingSubPlan stagePlan : plan.getChildren()) {
            List<StageExecutionAndScheduler> subTree = createStreamingLinkedStageExecutions(
                    stageExecution::addExchangeLocations,
                    stagePlan.withBucketToPartition(bucketToPartition),
                    nodeScheduler,
                    remoteTaskFactory,
                    splitSourceFactory,
                    session,
                    splitBatchSize,
                    partitioningCache,
                    nodePartitioningManager,
                    queryExecutor,
                    schedulerExecutor,
                    failureDetector,
                    nodeTaskMap,
                    tableWriteInfo,
                    Optional.of(stageExecution));
            stageExecutionInfos.addAll(subTree);
            childStagesBuilder.add(Iterables.getLast(subTree).getStageExecution());
        }
        Set<SqlStageExecution> childStageExecutions = childStagesBuilder.build();
        stageExecution.addStateChangeListener(newState -> {
            if (newState.isDone()) {
                childStageExecutions.forEach(SqlStageExecution::cancel);
            }
        });

        StageScheduler stageScheduler = createStageScheduler(
                plan,
                nodeScheduler,
                session,
                splitBatchSize,
                partitioningCache,
                nodePartitioningManager,
                schedulerExecutor,
                parentStageExecution,
                stageId,
                stageExecution,
                partitioningHandle,
                splitSources,
                childStageExecutions);
        StageLinkage stageLinkage = new StageLinkage(fragmentId, parent, childStageExecutions);
        stageExecutionInfos.add(new StageExecutionAndScheduler(stageExecution, stageLinkage, stageScheduler));

        return stageExecutionInfos.build();
    }

    private StageScheduler createStageScheduler(
            StreamingSubPlan plan,
            NodeScheduler nodeScheduler,
            Session session,
            int splitBatchSize,
            Function<PartitioningHandle, NodePartitionMap> partitioningCache,
            NodePartitioningManager nodePartitioningManager,
            ScheduledExecutorService schedulerExecutor,
            Optional<SqlStageExecution> parentStageExecution,
            StageId stageId, SqlStageExecution stageExecution,
            PartitioningHandle partitioningHandle,
            Map<PlanNodeId, SplitSource> splitSources,
            Set<SqlStageExecution> childStageExecutions)
    {
        int maxTasksPerStage = getMaxTasksPerStage(session);
        if (partitioningHandle.equals(SOURCE_DISTRIBUTION)) {
            // TODO: defer opening split sources when stage scheduling starts
            // nodes are selected dynamically based on the constraints of the splits and the system load
            Entry<PlanNodeId, SplitSource> entry = getOnlyElement(splitSources.entrySet());
            PlanNodeId planNodeId = entry.getKey();
            SplitSource splitSource = entry.getValue();
            ConnectorId connectorId = splitSource.getConnectorId();
            NodeSelector nodeSelector;
            if (isInternalSystemConnector(connectorId)) {
                nodeSelector = nodeScheduler.createLegacyNodeSelector(null, maxTasksPerStage);
            }
            else if (session.getSource().map(source -> source.contains("unidash")).orElse(false)) {
                nodeSelector = nodeScheduler.createNodeSelector(connectorId, maxTasksPerStage);
            }
            else {
                nodeSelector = nodeScheduler.createLegacyNodeSelector(connectorId, maxTasksPerStage);
            }

            SplitPlacementPolicy placementPolicy = new DynamicSplitPlacementPolicy(nodeSelector, stageExecution::getAllTasks);

            checkArgument(!plan.getFragment().getStageExecutionDescriptor().isStageGroupedExecution());
            return newSourcePartitionedSchedulerAsStageScheduler(stageExecution, planNodeId, splitSource, placementPolicy, splitBatchSize);
        }
        else if (partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
            Supplier<Collection<TaskStatus>> sourceTasksProvider = () -> childStageExecutions.stream()
                    .map(SqlStageExecution::getAllTasks)
                    .flatMap(Collection::stream)
                    .map(RemoteTask::getTaskStatus)
                    .collect(toList());

            Supplier<Collection<TaskStatus>> writerTasksProvider = () -> stageExecution.getAllTasks().stream()
                    .map(RemoteTask::getTaskStatus)
                    .collect(toList());

            ScaledWriterScheduler scheduler = new ScaledWriterScheduler(
                    stageExecution,
                    sourceTasksProvider,
                    writerTasksProvider,
                    nodeScheduler.createNodeSelector(null),
                    schedulerExecutor,
                    getWriterMinSize(session));
            whenAllStages(childStageExecutions, StageExecutionState::isDone)
                    .addListener(scheduler::finish, directExecutor());
            return scheduler;
        }
        else {
            // TODO: defer opening split sources when stage scheduling starts
            if (!splitSources.isEmpty()) {
                // contains local source
                List<PlanNodeId> schedulingOrder = plan.getFragment().getTableScanSchedulingOrder();
                ConnectorId connectorId = partitioningHandle.getConnectorId().orElseThrow(IllegalStateException::new);
                List<ConnectorPartitionHandle> connectorPartitionHandles;
                boolean groupedExecutionForStage = plan.getFragment().getStageExecutionDescriptor().isStageGroupedExecution();
                if (groupedExecutionForStage) {
                    connectorPartitionHandles = nodePartitioningManager.listPartitionHandles(session, partitioningHandle);
                    checkState(!ImmutableList.of(NOT_PARTITIONED).equals(connectorPartitionHandles));
                }
                else {
                    connectorPartitionHandles = ImmutableList.of(NOT_PARTITIONED);
                }

                BucketNodeMap bucketNodeMap;
                List<InternalNode> stageNodeList;
                if (plan.getFragment().getRemoteSourceNodes().stream().allMatch(node -> node.getExchangeType() == REPLICATE)) {
                    // no non-replicated remote source
                    boolean dynamicLifespanSchedule = plan.getFragment().getStageExecutionDescriptor().isDynamicLifespanSchedule();
                    bucketNodeMap = nodePartitioningManager.getBucketNodeMap(session, partitioningHandle, dynamicLifespanSchedule);

                    // verify execution is consistent with planner's decision on dynamic lifespan schedule
                    verify(bucketNodeMap.isDynamic() == dynamicLifespanSchedule);

                    if (!bucketNodeMap.isDynamic()) {
                        stageNodeList = ((FixedBucketNodeMap) bucketNodeMap).getBucketToNode().stream()
                                .distinct()
                                .collect(toImmutableList());
                    }
                    else {
                        stageNodeList = new ArrayList<>(nodeScheduler.createNodeSelector(connectorId).selectRandomNodes(maxTasksPerStage));
                    }
                }
                else {
                    // cannot use dynamic lifespan schedule
                    verify(!plan.getFragment().getStageExecutionDescriptor().isDynamicLifespanSchedule());

                    // remote source requires nodePartitionMap
                    NodePartitionMap nodePartitionMap = partitioningCache.apply(plan.getFragment().getPartitioning());
                    if (groupedExecutionForStage) {
                        checkState(connectorPartitionHandles.size() == nodePartitionMap.getBucketToPartition().length);
                    }
                    stageNodeList = nodePartitionMap.getPartitionToNode();
                    bucketNodeMap = nodePartitionMap.asBucketNodeMap();
                }

                FixedSourcePartitionedScheduler fixedSourcePartitionedScheduler = new FixedSourcePartitionedScheduler(
                        stageExecution,
                        splitSources,
                        plan.getFragment().getStageExecutionDescriptor(),
                        schedulingOrder,
                        stageNodeList,
                        bucketNodeMap,
                        splitBatchSize,
                        getConcurrentLifespansPerNode(session),
                        nodeScheduler.createNodeSelector(connectorId),
                        connectorPartitionHandles);
                if (plan.getFragment().getStageExecutionDescriptor().isRecoverableGroupedExecution()) {
                    stageExecution.registerStageTaskRecoveryCallback(taskId -> {
                        checkArgument(taskId.getStageExecutionId().getStageId().equals(stageId), "The task did not execute this stage");
                        checkArgument(parentStageExecution.isPresent(), "Parent stage execution must exist");
                        checkArgument(parentStageExecution.get().getAllTasks().size() == 1, "Parent stage should only have one task for recoverable grouped execution");

                        parentStageExecution.get().removeRemoteSourceIfSingleTaskStage(taskId);
                        fixedSourcePartitionedScheduler.recover(taskId);
                    });
                }
                return fixedSourcePartitionedScheduler;
            }

            else {
                // all sources are remote
                NodePartitionMap nodePartitionMap = partitioningCache.apply(plan.getFragment().getPartitioning());
                List<InternalNode> partitionToNode = nodePartitionMap.getPartitionToNode();
                // todo this should asynchronously wait a standard timeout period before failing
                checkCondition(!partitionToNode.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");
                return new FixedCountScheduler(stageExecution, partitionToNode);
            }
        }
    }

    private Optional<int[]> getBucketToPartition(
            PartitioningHandle partitioningHandle,
            Function<PartitioningHandle, NodePartitionMap> partitioningCache,
            Map<PlanNodeId, SplitSource> splitSources,
            List<RemoteSourceNode> remoteSourceNodes)
    {
        if (partitioningHandle.equals(SOURCE_DISTRIBUTION) || partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
            return Optional.of(new int[1]);
        }
        else if (!splitSources.isEmpty()) {
            if (remoteSourceNodes.stream().allMatch(node -> node.getExchangeType() == REPLICATE)) {
                return Optional.empty();
            }
            else {
                // remote source requires nodePartitionMap
                NodePartitionMap nodePartitionMap = partitioningCache.apply(partitioningHandle);
                return Optional.of(nodePartitionMap.getBucketToPartition());
            }
        }
        else {
            NodePartitionMap nodePartitionMap = partitioningCache.apply(partitioningHandle);
            List<InternalNode> partitionToNode = nodePartitionMap.getPartitionToNode();
            // todo this should asynchronously wait a standard timeout period before failing
            checkCondition(!partitionToNode.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");
            return Optional.of(nodePartitionMap.getBucketToPartition());
        }
    }

    public BasicStageExecutionStats getBasicStageStats()
    {
        List<BasicStageExecutionStats> stageStats = stageExecutions.values().stream()
                .map(stageExecutionInfo -> stageExecutionInfo.getStageExecution().getBasicStageStats())
                .collect(toImmutableList());

        return aggregateBasicStageStats(stageStats);
    }

    public StageInfo getStageInfo()
    {
        Map<StageId, StageExecutionInfo> stageInfos = stageExecutions.values().stream()
                .map(stageExecutionInfo -> stageExecutionInfo.getStageExecution().getStageExecutionInfo())
                .collect(toImmutableMap(execution -> execution.getStageExecutionId().getStageId(), identity()));

        return buildStageInfo(plan, stageInfos);
    }

    private StageInfo buildStageInfo(SubPlan subPlan, Map<StageId, StageExecutionInfo> stageExecutionInfos)
    {
        StageId stageId = getStageId(subPlan.getFragment().getId());
        StageExecutionInfo stageExecutionInfo = stageExecutionInfos.get(stageId);
        checkArgument(stageExecutionInfo != null, "No stageExecutionInfo for %s", stageId);
        return new StageInfo(
                stageId,
                locationFactory.createStageLocation(stageId),
                Optional.of(subPlan.getFragment()),
                stageExecutionInfo,
                ImmutableList.of(),
                subPlan.getChildren().stream()
                        .map(plan -> buildStageInfo(plan, stageExecutionInfos))
                        .collect(toImmutableList()));
    }

    public long getUserMemoryReservation()
    {
        return stageExecutions.values().stream()
                .mapToLong(stageExecutionInfo -> stageExecutionInfo.getStageExecution().getUserMemoryReservation())
                .sum();
    }

    public long getTotalMemoryReservation()
    {
        return stageExecutions.values().stream()
                .mapToLong(stageExecutionInfo -> stageExecutionInfo.getStageExecution().getTotalMemoryReservation())
                .sum();
    }

    public Duration getTotalCpuTime()
    {
        long millis = stageExecutions.values().stream()
                .mapToLong(stage -> stage.getStageExecution().getTotalCpuTime().toMillis())
                .sum();
        return new Duration(millis, MILLISECONDS);
    }
>>>>>>> Add affinity scheduling for unidash

    public void start()
    {
        if (started.compareAndSet(false, true)) {
            startScheduling();
        }
    }

    private void startScheduling()
    {
        // still scheduling the previous batch of stages
        if (scheduling.get()) {
            return;
        }
        executor.submit(this::schedule);
    }

    private void schedule()
    {
        if (!scheduling.compareAndSet(false, true)) {
            // still scheduling the previous batch of stages
            return;
        }

        List<StageExecutionAndScheduler> scheduledStageExecutions = new ArrayList<>();

        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            Set<StageId> completedStages = new HashSet<>();

            List<ExecutionSchedule> executionSchedules = new LinkedList<>();

            while (!Thread.currentThread().isInterrupted()) {
                // remove finished section
                executionSchedules.removeIf(ExecutionSchedule::isFinished);

                // try to pull more section that are ready to be run
                List<StreamingPlanSection> sectionsReadyForExecution = getSectionsReadyForExecution();

                // all finished
                if (sectionsReadyForExecution.isEmpty() && executionSchedules.isEmpty()) {
                    break;
                }

                List<SectionExecution> sectionExecutions = createStageExecutions(sectionsReadyForExecution);
                if (queryStateMachine.isDone()) {
                    sectionExecutions.forEach(SectionExecution::abort);
                }

                sectionExecutions.forEach(sectionExecution -> scheduledStageExecutions.addAll(sectionExecution.getSectionStages()));
                sectionExecutions.stream()
                        .map(SectionExecution::getSectionStages)
                        .map(executionPolicy::createExecutionSchedule)
                        .forEach(executionSchedules::add);

                while (!executionSchedules.isEmpty() && executionSchedules.stream().noneMatch(ExecutionSchedule::isFinished)) {
                    List<ListenableFuture<?>> blockedStages = new ArrayList<>();

                    List<StageExecutionAndScheduler> executionsToSchedule = executionSchedules.stream()
                            .flatMap(schedule -> schedule.getStagesToSchedule().stream())
                            .collect(toImmutableList());

                    for (StageExecutionAndScheduler executionAndScheduler : executionsToSchedule) {
                        executionAndScheduler.getStageExecution().beginScheduling();

                        // perform some scheduling work
                        ScheduleResult result = executionAndScheduler.getStageScheduler()
                                .schedule();

                        // modify parent and children based on the results of the scheduling
                        if (result.isFinished()) {
                            executionAndScheduler.getStageExecution().schedulingComplete();
                        }
                        else if (!result.getBlocked().isDone()) {
                            blockedStages.add(result.getBlocked());
                        }
                        executionAndScheduler.getStageLinkage()
                                .processScheduleResults(executionAndScheduler.getStageExecution().getState(), result.getNewTasks());
                        schedulerStats.getSplitsScheduledPerIteration().add(result.getSplitsScheduled());
                        if (result.getBlockedReason().isPresent()) {
                            switch (result.getBlockedReason().get()) {
                                case WRITER_SCALING:
                                    // no-op
                                    break;
                                case WAITING_FOR_SOURCE:
                                    schedulerStats.getWaitingForSource().update(1);
                                    break;
                                case SPLIT_QUEUES_FULL:
                                    schedulerStats.getSplitQueuesFull().update(1);
                                    break;
                                case MIXED_SPLIT_QUEUES_FULL_AND_WAITING_FOR_SOURCE:
                                    schedulerStats.getMixedSplitQueuesFullAndWaitingForSource().update(1);
                                    break;
                                case NO_ACTIVE_DRIVER_GROUP:
                                    schedulerStats.getNoActiveDriverGroup().update(1);
                                    break;
                                default:
                                    throw new UnsupportedOperationException("Unknown blocked reason: " + result.getBlockedReason().get());
                            }
                        }
                    }

                    // make sure to update stage linkage at least once per loop to catch async state changes (e.g., partial cancel)
                    boolean stageFinishedExecution = false;
                    for (StageExecutionAndScheduler stageExecutionAndScheduler : scheduledStageExecutions) {
                        SqlStageExecution stageExecution = stageExecutionAndScheduler.getStageExecution();
                        StageId stageId = stageExecution.getStageExecutionId().getStageId();
                        if (!completedStages.contains(stageId) && stageExecution.getState().isDone()) {
                            stageExecutionAndScheduler.getStageLinkage()
                                    .processScheduleResults(stageExecution.getState(), ImmutableSet.of());
                            completedStages.add(stageId);
                            stageFinishedExecution = true;
                        }
                    }

                    // if any stage has just finished execution try to pull more sections for scheduling
                    if (stageFinishedExecution) {
                        break;
                    }

                    // wait for a state change and then schedule again
                    if (!blockedStages.isEmpty()) {
                        try (TimeStat.BlockTimer timer = schedulerStats.getSleepTime().time()) {
                            tryGetFutureValue(whenAnyComplete(blockedStages), 1, SECONDS);
                        }
                        for (ListenableFuture<?> blockedStage : blockedStages) {
                            blockedStage.cancel(true);
                        }
                    }
                }
            }

            for (StageExecutionAndScheduler stageExecutionAndScheduler : scheduledStageExecutions) {
                StageExecutionState state = stageExecutionAndScheduler.getStageExecution().getState();
                if (state != SCHEDULED && state != RUNNING && !state.isDone()) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Scheduling is complete, but stage execution %s is in state %s", stageExecutionAndScheduler.getStageExecution().getStageExecutionId(), state));
                }
            }

            scheduling.set(false);

            if (!getSectionsReadyForExecution().isEmpty()) {
                startScheduling();
            }
        }
        catch (Throwable t) {
            scheduling.set(false);
            queryStateMachine.transitionToFailed(t);
            throw t;
        }
        finally {
            RuntimeException closeError = new RuntimeException();
            for (StageExecutionAndScheduler stageExecutionAndScheduler : scheduledStageExecutions) {
                try {
                    stageExecutionAndScheduler.getStageScheduler().close();
                }
                catch (Throwable t) {
                    queryStateMachine.transitionToFailed(t);
                    // Self-suppression not permitted
                    if (closeError != t) {
                        closeError.addSuppressed(t);
                    }
                }
            }
            if (closeError.getSuppressed().length > 0) {
                throw closeError;
            }
        }
    }

    private List<StreamingPlanSection> getSectionsReadyForExecution()
    {
        long runningPlanSections =
                stream(forTree(StreamingPlanSection::getChildren).depthFirstPreOrder(sectionedPlan))
                        .map(section -> getLatestSectionExecution(getStageId(section.getPlan().getFragment().getId())))
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .filter(SectionExecution::isRunning)
                        .count();
        return stream(forTree(StreamingPlanSection::getChildren).depthFirstPreOrder(sectionedPlan))
                // get all sections ready for execution
                .filter(this::isReadyForExecution)
                .limit(maxConcurrentMaterializations - runningPlanSections)
                .collect(toImmutableList());
    }

    private boolean isReadyForExecution(StreamingPlanSection section)
    {
        Optional<SectionExecution> sectionExecution = getLatestSectionExecution(getStageId(section.getPlan().getFragment().getId()));
        if (sectionExecution.isPresent() && (sectionExecution.get().isRunning() || sectionExecution.get().isFinished())) {
            // already scheduled
            return false;
        }
        for (StreamingPlanSection child : section.getChildren()) {
            Optional<SectionExecution> childSectionExecution = getLatestSectionExecution(getStageId(child.getPlan().getFragment().getId()));
            if (!childSectionExecution.isPresent() || !childSectionExecution.get().isFinished()) {
                return false;
            }
        }
        return true;
    }

    private Optional<SectionExecution> getLatestSectionExecution(StageId stageId)
    {
        List<SectionExecution> sectionExecutions = this.sectionExecutions.get(stageId);
        if (sectionExecutions == null || sectionExecutions.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(getLast(sectionExecutions));
    }

    private StageId getStageId(PlanFragmentId fragmentId)
    {
        return new StageId(session.getQueryId(), fragmentId.getId());
    }

    private List<SectionExecution> createStageExecutions(List<StreamingPlanSection> sections)
    {
        ImmutableList.Builder<SectionExecution> result = ImmutableList.builder();
        for (StreamingPlanSection section : sections) {
            StageId sectionId = getStageId(section.getPlan().getFragment().getId());
            List<SectionExecution> attempts = sectionExecutions.computeIfAbsent(sectionId, (ignored) -> new CopyOnWriteArrayList<>());

            // sectionExecutions only get created when they are about to be scheduled, so there should
            // never be a non-failed SectionExecution for a section that's ready for execution
            verify(attempts.isEmpty() || getLast(attempts).isFailed(), "Non-failed sectionExecutions already exists");

            PlanFragment sectionRootFragment = section.getPlan().getFragment();

            Optional<int[]> bucketToPartition;
            OutputBuffers outputBuffers;
            ExchangeLocationsConsumer locationsConsumer;
            if (isRootFragment(sectionRootFragment)) {
                bucketToPartition = Optional.of(new int[1]);
                outputBuffers = createInitialEmptyOutputBuffers(sectionRootFragment.getPartitioningScheme().getPartitioning().getHandle())
                        .withBuffer(new OutputBufferId(0), BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds();
                OutputBufferId rootBufferId = getOnlyElement(outputBuffers.getBuffers().keySet());
                locationsConsumer = (fragmentId, tasks, noMoreExchangeLocations) ->
                        updateQueryOutputLocations(queryStateMachine, rootBufferId, tasks, noMoreExchangeLocations);
            }
            else {
                bucketToPartition = Optional.empty();
                outputBuffers = createDiscardingOutputBuffers();
                locationsConsumer = (fragmentId, tasks, noMoreExchangeLocations) -> {};
            }

            int attemptId = attempts.size();
            SectionExecution sectionExecution = sectionExecutionFactory.createSectionExecutions(
                    session,
                    section,
                    locationsConsumer,
                    bucketToPartition,
                    outputBuffers,
                    summarizeTaskInfo,
                    remoteTaskFactory,
                    splitSourceFactory,
                    attemptId);

            addStateChangeListeners(sectionExecution);
            attempts.add(sectionExecution);
            result.add(sectionExecution);
        }
        return result.build();
    }

    private static void updateQueryOutputLocations(QueryStateMachine queryStateMachine, OutputBufferId rootBufferId, Set<RemoteTask> tasks, boolean noMoreExchangeLocations)
    {
        Map<URI, TaskId> bufferLocations = tasks.stream()
                .collect(toImmutableMap(
                        task -> getBufferLocation(task, rootBufferId),
                        RemoteTask::getTaskId));
        queryStateMachine.updateOutputLocations(bufferLocations, noMoreExchangeLocations);
    }

    private static URI getBufferLocation(RemoteTask remoteTask, OutputBufferId rootBufferId)
    {
        URI location = remoteTask.getTaskStatus().getSelf();
        return uriBuilderFrom(location).appendPath("results").appendPath(rootBufferId.toString()).build();
    }

    private void addStateChangeListeners(SectionExecution sectionExecution)
    {
        for (StageExecutionAndScheduler stageExecutionAndScheduler : sectionExecution.getSectionStages()) {
            SqlStageExecution stageExecution = stageExecutionAndScheduler.getStageExecution();
            if (isRootFragment(stageExecution.getFragment())) {
                stageExecution.addStateChangeListener(state -> {
                    if (state == FINISHED) {
                        queryStateMachine.transitionToFinishing();
                    }
                    else if (state == CANCELED) {
                        // output stage was canceled
                        queryStateMachine.transitionToCanceled();
                    }
                });
            }
            stageExecution.addStateChangeListener(state -> {
                if (queryStateMachine.isDone()) {
                    return;
                }

                if (state == FAILED) {
                    ExecutionFailureInfo failureInfo = stageExecution.getStageExecutionInfo().getFailureCause()
                            .orElseThrow(() -> new VerifyException(format("stage execution failed, but the failure info is missing: %s", stageExecution.getStageExecutionId())));
                    Exception failureException = failureInfo.toException();

                    boolean isRootSection = isRootFragment(sectionExecution.getRootStage().getStageExecution().getFragment());
                    // root section directly streams the results to the user, cannot be retried
                    if (isRootSection) {
                        queryStateMachine.transitionToFailed(failureException);
                        return;
                    }

                    if (retriedSections.get() >= maxStageRetries) {
                        queryStateMachine.transitionToFailed(failureException);
                        return;
                    }

                    if (!RECOVERABLE_ERROR_CODES.contains(failureInfo.getErrorCode())) {
                        queryStateMachine.transitionToFailed(failureException);
                        return;
                    }

                    try {
                        if (sectionExecution.abort()) {
                            retriedSections.incrementAndGet();
                            nodeManager.refreshNodes();
                            startScheduling();
                        }
                    }
                    catch (Throwable t) {
                        if (failureException != t) {
                            failureException.addSuppressed(t);
                        }
                        queryStateMachine.transitionToFailed(failureException);
                    }
                }
                else if (state == FINISHED) {
                    // checks if there's any new sections available for execution and starts the scheduling if any
                    startScheduling();
                }
                else if (queryStateMachine.getQueryState() == QueryState.STARTING) {
                    // if the stage has at least one task, we are running
                    if (stageExecution.hasTasks()) {
                        queryStateMachine.transitionToRunning();
                    }
                }
            });
            stageExecution.addFinalStageInfoListener(status -> queryStateMachine.updateQueryInfo(Optional.of(getStageInfo())));
        }
    }

    private static boolean isRootFragment(PlanFragment fragment)
    {
        return fragment.getId().getId() == ROOT_FRAGMENT_ID;
    }

    @Override
    public long getUserMemoryReservation()
    {
        return getAllStagesExecutions().mapToLong(SqlStageExecution::getUserMemoryReservation).sum();
    }

    @Override
    public long getTotalMemoryReservation()
    {
        return getAllStagesExecutions().mapToLong(SqlStageExecution::getTotalMemoryReservation).sum();
    }

    @Override
    public Duration getTotalCpuTime()
    {
        long millis = getAllStagesExecutions()
                .map(SqlStageExecution::getTotalCpuTime)
                .mapToLong(Duration::toMillis)
                .sum();
        return new Duration(millis, MILLISECONDS);
    }

    @Override
    public BasicStageExecutionStats getBasicStageStats()
    {
        List<BasicStageExecutionStats> stageStats = getAllStagesExecutions()
                .map(SqlStageExecution::getBasicStageStats)
                .collect(toImmutableList());
        return aggregateBasicStageStats(stageStats);
    }

    private Stream<SqlStageExecution> getAllStagesExecutions()
    {
        return sectionExecutions.values().stream()
                .flatMap(Collection::stream)
                .flatMap(sectionExecution -> sectionExecution.getSectionStages().stream())
                .map(StageExecutionAndScheduler::getStageExecution);
    }

    @Override
    public StageInfo getStageInfo()
    {
        ListMultimap<StageId, SqlStageExecution> stageExecutions = getStageExecutions();
        return buildStageInfo(plan, stageExecutions);
    }

    private StageInfo buildStageInfo(SubPlan subPlan, ListMultimap<StageId, SqlStageExecution> stageExecutions)
    {
        StageId stageId = getStageId(subPlan.getFragment().getId());
        List<SqlStageExecution> attempts = stageExecutions.get(stageId);

        StageExecutionInfo latestAttemptInfo = attempts.isEmpty() ?
                unscheduledExecutionInfo(stageId.getId(), queryStateMachine.isDone()) :
                getLast(attempts).getStageExecutionInfo();
        List<StageExecutionInfo> previousAttemptInfos = attempts.size() < 2 ?
                ImmutableList.of() :
                attempts.subList(0, attempts.size() - 1).stream()
                        .map(SqlStageExecution::getStageExecutionInfo)
                        .collect(toImmutableList());

        return new StageInfo(
                stageId,
                locationFactory.createStageLocation(stageId),
                Optional.of(subPlan.getFragment()),
                latestAttemptInfo,
                previousAttemptInfos,
                subPlan.getChildren().stream()
                        .map(plan -> buildStageInfo(plan, stageExecutions))
                        .collect(toImmutableList()));
    }

    private ListMultimap<StageId, SqlStageExecution> getStageExecutions()
    {
        ImmutableListMultimap.Builder<StageId, SqlStageExecution> result = ImmutableListMultimap.builder();
        for (Collection<SectionExecution> sectionExecutionAttempts : sectionExecutions.values()) {
            for (SectionExecution sectionExecution : sectionExecutionAttempts) {
                for (StageExecutionAndScheduler stageExecution : sectionExecution.getSectionStages()) {
                    result.put(stageExecution.getStageExecution().getStageExecutionId().getStageId(), stageExecution.getStageExecution());
                }
            }
        }
        return result.build();
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            getAllStagesExecutions()
                    .filter(execution -> execution.getStageExecutionId().getStageId().equals(stageId))
                    .forEach(SqlStageExecution::cancel);
        }
    }

    @Override
    public void abort()
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            checkState(queryStateMachine.isDone(), "query scheduler is expected to be aborted only if the query is finished: %s", queryStateMachine.getQueryState());
            getAllStagesExecutions().forEach(SqlStageExecution::abort);
        }
    }
}

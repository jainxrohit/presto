package com.facebook.presto.operator;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.sql.planner.LocalExecutionPlanner.FragmentCacheKey;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.sql.planner.LocalExecutionPlanner.fragmentCache;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class DriverSplitCacheOperator
        implements Operator
{
    public static class DriverSplitCacheOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNode planNode;

        private boolean closed;

        public DriverSplitCacheOperatorFactory(
                int operatorId,
                PlanNode planNode)
        {
            this.operatorId = operatorId;
            this.planNode = requireNonNull(planNode, "planNodeId is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            return new DriverSplitCacheOperator(
                    driverContext.addOperatorContext(operatorId, planNode.getId(), DriverSplitCacheOperator.class.getSimpleName()),
                    planNode);
        }

        @Override
        public void noMoreOperators()
        {
            checkState(!closed, "Factory is already closed");
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("duplicate() is not supported for DynamicFilterSourceOperatorFactory");
        }
    }

    private final OperatorContext context;
    private final PlanNode planNode;

    private boolean finished;
    private Map<Split, List<Page>> splitPages = new HashMap<>();
    private Page current;

    private DriverSplitCacheOperator(
            OperatorContext context,
            PlanNode planNode)
    {
        this.context = requireNonNull(context, "context is null");
        this.planNode = planNode;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return context;
    }

    @Override
    public boolean needsInput()
    {
        return current == null && !finished;
    }

    @Override
    public void addInput(Page page)
    {
        verify(!finished, "DynamicFilterSourceOperator: addInput() shouldn't not be called after finish()");
        current = page;
        Block splitBlock = page.getBlock(page.getChannelCount() - 1);
        Split split = jsonCodec(Split.class).fromJson(splitBlock.getSlice(0, 0, splitBlock.getSliceLength(0)).getBytes());
    }

    @Override
    public Page getOutput()
    {
        Page result = current;
        current = null;
        return result;
    }

    @Override
    public void finish()
    {
        if (finished) {
            // NOTE: finish() may be called multiple times (see comment at Driver::processInternal).
            return;
        }
        finished = true;
        fragmentCache.put(new FragmentCacheKey(planNode, null), null);
    }

    @Override
    public boolean isFinished()
    {
        return current == null && finished;
    }
}

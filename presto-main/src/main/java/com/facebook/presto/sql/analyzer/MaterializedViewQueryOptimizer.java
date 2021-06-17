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
package com.facebook.presto.sql.analyzer;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class MaterializedViewQueryOptimizer
        extends AstVisitor<Node, MaterializedViewQueryOptimizer.MaterializedViewQueryOptimizerContext>
{
    private static final Logger logger = Logger.get(MaterializedViewQueryOptimizer.class);

    public Node rewrite(Node node, MaterializedViewQueryOptimizerContext context)
    {
        try {
            return process(node, context);
        }
        catch (IllegalStateException ex) {
            logger.error(ex, "Failed to rewrite query: %s.", node);
            return node;
        }
    }

    @Override
    public Node process(Node node, MaterializedViewQueryOptimizerContext context)
    {
        return super.process(node, context);
    }

    @Override
    public Node visitNode(Node node, MaterializedViewQueryOptimizerContext context)
    {
        return node;
    }

    @Override
    protected Node visitQuery(Query node, MaterializedViewQueryOptimizerContext context)
    {
        return new Query(
                node.getWith(),
                (QueryBody) process(node.getQueryBody(), context),
                node.getOrderBy(),
                node.getLimit());
    }

    @Override
    protected Node visitTableSubquery(TableSubquery node, MaterializedViewQueryOptimizerContext context)
    {
        return new TableSubquery((Query) process(node.getQuery(), context));
    }

    @Override
    protected Node visitQuerySpecification(QuerySpecification node, MaterializedViewQueryOptimizerContext context)
    {
        // Process from and select clause before all others to check if query is rewritable and acquire alias relation if any
        Optional<Relation> rewrittenFrom = node.getFrom().isPresent() ? Optional.of((Relation) process(node.getFrom().get(), context)) : Optional.empty();
        Select rewrittenSelect = (Select) process(node.getSelect(), context);

        Optional<Expression> rewrittenWhere = node.getWhere().isPresent() ? Optional.of((Expression) process(node.getWhere().get(), context)) : Optional.empty();
        Optional<GroupBy> rewrittenGroupBy = node.getGroupBy().isPresent() ? Optional.of((GroupBy) process(node.getGroupBy().get(), context)) : Optional.empty();
        Optional<Expression> rewrittenHaving = node.getHaving().isPresent() ? Optional.of((Expression) process(node.getHaving().get(), context)) : Optional.empty();
        Optional<OrderBy> rewrittenOrderBy = node.getOrderBy().isPresent() ? Optional.of((OrderBy) process(node.getOrderBy().get(), context)) : Optional.empty();

        return new QuerySpecification(
                rewrittenSelect,
                rewrittenFrom,
                rewrittenWhere,
                rewrittenGroupBy,
                rewrittenHaving,
                rewrittenOrderBy,
                node.getLimit());
    }

    @Override
    protected Node visitSelect(Select node, MaterializedViewQueryOptimizerContext context)
    {
        ImmutableList.Builder<SelectItem> rewrittenSelectItems = ImmutableList.builder();

        for (SelectItem selectItem : node.getSelectItems()) {
            SelectItem rewrittenSelectItem = (SelectItem) process(selectItem, context);
            rewrittenSelectItems.add(rewrittenSelectItem);
        }

        return new Select(node.isDistinct(), rewrittenSelectItems.build());
    }

    @Override
    protected Node visitSingleColumn(SingleColumn node, MaterializedViewQueryOptimizerContext context)
    {
        return new SingleColumn(
                (Expression) process(node.getExpression(), context),
                node.getAlias());
    }

    @Override
    protected Node visitAllColumns(AllColumns node, MaterializedViewQueryOptimizerContext context)
    {
        return node;
    }

    @Override
    protected Node visitArithmeticBinary(ArithmeticBinaryExpression node, MaterializedViewQueryOptimizerContext context)
    {
        return new ArithmeticBinaryExpression(
                node.getOperator(),
                (Expression) process(node.getLeft(), context),
                (Expression) process(node.getRight(), context));
    }

    @Override
    protected Node visitIdentifier(Identifier node, MaterializedViewQueryOptimizerContext context)
    {
        String baseColumnName = node.getValue();
        return new Identifier(context.getViewColumnName(baseColumnName), node.isDelimited());
    }

    @Override
    protected Node visitFunctionCall(FunctionCall node, MaterializedViewQueryOptimizerContext context)
    {
        String functionCall = node.toString();
        ImmutableList.Builder<Expression> rewrittenArguments = ImmutableList.builder();

        if (context.containsColumnName(functionCall)) {
            Expression derivedExpression = new Identifier(context.getViewColumnName(functionCall));
            rewrittenArguments.add(derivedExpression);
        }
        else {
            for (Expression argument : node.getArguments()) {
                Expression rewrittenArgument = (Expression) process(argument, context);
                rewrittenArguments.add(rewrittenArgument);
            }
        }

        return new FunctionCall(
                node.getName(),
                node.getWindow(),
                node.getFilter(),
                node.getOrderBy(),
                node.isDistinct(),
                node.isIgnoreNulls(),
                rewrittenArguments.build());
    }

    // Assuming the current base query applies to this specific materialized view
    @Override
    protected Node visitRelation(Relation node, MaterializedViewQueryOptimizerContext context)
    {
        Optional<Relation> baseQueryFrom = context.getBaseQueryFrom();
        if (baseQueryFrom.isPresent() && node.equals(baseQueryFrom.get())) {
            return context.getMaterializedView();
        }
        return node;
    }

    @Override
    protected Node visitLogicalBinaryExpression(LogicalBinaryExpression node, MaterializedViewQueryOptimizerContext context)
    {
        return new LogicalBinaryExpression(
                node.getOperator(),
                (Expression) process(node.getLeft(), context),
                (Expression) process(node.getRight(), context));
    }

    @Override
    protected Node visitComparisonExpression(ComparisonExpression node, MaterializedViewQueryOptimizerContext context)
    {
        return new ComparisonExpression(
                node.getOperator(),
                (Expression) process(node.getLeft(), context),
                (Expression) process(node.getRight(), context));
    }

    @Override
    protected Node visitLiteral(Literal node, MaterializedViewQueryOptimizerContext context)
    {
        return node;
    }

    @Override
    protected Node visitGroupBy(GroupBy node, MaterializedViewQueryOptimizerContext context)
    {
        ImmutableList.Builder<GroupingElement> rewrittenGroupBy = ImmutableList.builder();
        for (GroupingElement element : node.getGroupingElements()) {
            GroupingElement rewrittenElement = (GroupingElement) process(element, context);
            rewrittenGroupBy.add(rewrittenElement);
        }
        return new GroupBy(
                node.isDistinct(),
                rewrittenGroupBy.build());
    }

    @Override
    protected Node visitOrderBy(OrderBy node, MaterializedViewQueryOptimizerContext context)
    {
        ImmutableList.Builder<SortItem> rewrittenOrderBy = ImmutableList.builder();
        for (SortItem sortItem : node.getSortItems()) {
            SortItem rewrittenSortItem = (SortItem) process(sortItem, context);
            rewrittenOrderBy.add(rewrittenSortItem);
        }
        return new OrderBy(rewrittenOrderBy.build());
    }

    @Override
    protected Node visitSortItem(SortItem node, MaterializedViewQueryOptimizerContext context)
    {
        return new SortItem(
                (Expression) process(node.getSortKey(), context),
                node.getOrdering(),
                node.getNullOrdering());
    }

    @Override
    protected Node visitSimpleGroupBy(SimpleGroupBy node, MaterializedViewQueryOptimizerContext context)
    {
        ImmutableList.Builder<Expression> rewrittenSimpleGroupBy = ImmutableList.builder();
        for (Expression column : node.getExpressions()) {
            Expression rewrittenColumn = (Expression) process(column, context);
            rewrittenSimpleGroupBy.add(rewrittenColumn);
        }
        return new SimpleGroupBy(rewrittenSimpleGroupBy.build());
    }

    public static final class MaterializedViewQueryOptimizerContext
    {
        private final Table materializedView;
        private final ImmutableMap<String, String> baseToViewColumnMap;
        private final Optional<Relation> baseQueryRelation;

        public MaterializedViewQueryOptimizerContext(
                Table materializedView,
                Query originalSqlQuery)
        {
            this.materializedView = requireNonNull(materializedView, "materialized view is null");

            QuerySpecification originalSqlQueryBody = (QuerySpecification) originalSqlQuery.getQueryBody();
            this.baseQueryRelation = originalSqlQueryBody.getFrom().isPresent() ? originalSqlQueryBody.getFrom() : Optional.empty();

            // Create base to materialized view column map
            ImmutableMap.Builder<String, String> baseToViewColumnMap = ImmutableMap.builder();

            for (SelectItem baseTableSelectColumn : originalSqlQueryBody.getSelect().getSelectItems()) {
                if (baseTableSelectColumn instanceof SingleColumn) {
                    String baseColumnName = ((SingleColumn) baseTableSelectColumn).getExpression().toString();
                    Optional<Identifier> baseColumnAlias = ((SingleColumn) baseTableSelectColumn).getAlias();
                    String viewDerivedColumnName = baseColumnAlias.orElse(new Identifier(baseColumnName)).getValue();

                    baseToViewColumnMap.put(baseColumnName, viewDerivedColumnName);
                }
            }

            this.baseToViewColumnMap = baseToViewColumnMap.build();
        }

        public Table getMaterializedView()
        {
            return materializedView;
        }

        public String getViewColumnName(String baseColumnName)
        {
            if (!containsColumnName(baseColumnName)) {
                throw new IllegalStateException("Failed to find the base column: " + baseColumnName);
            }
            return baseToViewColumnMap.getOrDefault(baseColumnName, baseColumnName);
        }

        public boolean containsColumnName(String baseColumnName)
        {
            return baseToViewColumnMap.containsKey(baseColumnName);
        }

        public Optional<Relation> getBaseQueryFrom()
        {
            return baseQueryRelation;
        }
    }
}

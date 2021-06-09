package com.facebook.presto.sql.rewrite;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.ConnectorMaterializedViewDefinition;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.sql.analyzer.MaterializedViewQueryOptimizer;
import com.facebook.presto.sql.analyzer.MaterializedViewValidationForQueryRewrite;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.analyzer.MaterializedViewQueryOptimizer.MaterializedViewQueryOptimizerContext;
import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static java.util.Objects.requireNonNull;

public class MaterializedViewOptimizationRewriter
        implements StatementRewrite.Rewrite
{
    @Override
    public Statement rewrite(
            Session session,
            Metadata metadata,
            SqlParser parser,
            Optional<QueryExplainer> queryExplainer,
            Statement node,
            List<Expression> parameters,
            AccessControl accessControl,
            WarningCollector warningCollector)
    {
        return (Statement) new MaterializedViewOptimizationRewriter.Visitor(metadata, session, parser).process(node, null);
    }

    private static final class Visitor
            extends AstVisitor<Node, Void>
    {
        private final Metadata metadata;
        private final Session session;
        private final SqlParser sqlParser;

        public Visitor(
                Metadata metadata,
                Session session,
                SqlParser parser)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.session = requireNonNull(session, "session is null");
            this.sqlParser = requireNonNull(parser, "queryPreparer is null");
        }

        @Override
        protected Node visitNode(Node node, Void context)
        {
            return node;
        }

        protected Node visitQuery(Query query, Void context)
        {
            if (SystemSessionProperties.isQueryOptimizationWithMaterializedViewEnabled(session)) {
                Optional<Query> optimizeQuery = optimizeQueryUsingMaterializedView(query);
                if (optimizeQuery.isPresent()) {
                    return optimizeQuery.get();
                }
            }

            return query;
        }

        // TODO: move this to utility
        private Optional<Query> optimizeQueryUsingMaterializedView(Query node)
        {
            Map<String, List<String>> baseTableToMaterializedViewMapping = getBaseToMaterializedViewMapping();
            MaterializedViewValidationForQueryRewrite<Object, Object> rewriteValidator = new MaterializedViewValidationForQueryRewrite<>(baseTableToMaterializedViewMapping);
            rewriteValidator.process(node);
            Set<String> materializedViewCandidate = rewriteValidator.generateMaterializedViewCandidate();
            if (materializedViewCandidate.isEmpty()) {
                return Optional.empty();
            }
            Query rewriteQuery = getQueryWithMaterializedViewOptimization(node, materializedViewCandidate.iterator().next());
            return Optional.of(rewriteQuery);
        }

        // TODO: move this to utility
        private Query getQueryWithMaterializedViewOptimization(
                Query statement,
                String materializedViewName)
        {
            Table materializedViewTable = new Table(QualifiedName.of(materializedViewName));
            QualifiedObjectName materializedViewQualifiedObjectName = createQualifiedObjectName(session, materializedViewTable, materializedViewTable.getName());
            Optional<ConnectorMaterializedViewDefinition> materializedView = metadata.getMaterializedView(session, materializedViewQualifiedObjectName);

            Query originalViewQuery = (Query) sqlParser.createStatement(materializedView.get().getOriginalSql());
            Query rewriteBaseToViewQuery = (Query) new MaterializedViewQueryOptimizer(session).process(statement, new MaterializedViewQueryOptimizerContext(materializedViewTable, originalViewQuery));

            // Temporarily it returns the first rewrite query
            // TODO: Find the appropriate materialized view for a base query
            return rewriteBaseToViewQuery;
        }

        // TODO: The mapping should be fetched from metastore
        private Map<String, List<String>> getBaseToMaterializedViewMapping()
        {
            Map<String, List<String>> baseTableToMaterializedViewMap = new HashMap<>();
            baseTableToMaterializedViewMap.put("lineitem_partitioned_derived_fields", ImmutableList.of("lineitem_partitioned_view_derived_fields"));
            return baseTableToMaterializedViewMap;
        }
    }
}
package com.facebook.presto.sql.rewrite;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isOptimizeUnionToUnionAll;
import static java.util.Objects.requireNonNull;

public class UnionOptimizationRewrite
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
            Map<NodeRef<Parameter>, Expression> parameterLookup,
            AccessControl accessControl,
            WarningCollector warningCollector)
    {
        if (isOptimizeUnionToUnionAll(session)) {
            return (Statement) new Rewriter(metadata, session, parser, accessControl)
                    .process(node, null);
        }

        return node;
    }

    private static final class Rewriter
            extends AstVisitor<Node, Void>
    {
        private final Metadata metadata;
        private final Session session;
        private final SqlParser sqlParser;
        private final AccessControl accessControl;

        public Rewriter(
                Metadata metadata,
                Session session,
                SqlParser parser,
                AccessControl accessControl)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.session = requireNonNull(session, "session is null");
            this.sqlParser = requireNonNull(parser, "queryPreparer is null");
            this.accessControl = requireNonNull(accessControl, "access control is null");
        }

        @Override
        protected Node visitNode(Node node, Void context)
        {
            for (Node child : node.getChildren()) {
                process(child, context);
            }
            return node;
        }

        protected Node visitQuery(Query node, Void context)
        {
            QueryBody newQueryBody = processSameType(node.getQueryBody());
            Optional<With> newWith = node.getWith().map(this::processSameType);
            boolean withSame = !newWith.isPresent() || node.getWith().get() == newWith.get();
            if (withSame && node.getQueryBody() == newQueryBody) {
                return node;
            }

            return new Query(newWith, newQueryBody, node.getOrderBy(), node.getOffset(), node.getLimit());
        }

        @Override
        protected Node visitUnion(Union node, Void context)
        {
            if (node.isDistinct().orElse(false)) {
                return node;
            }

            return new Union(node.getRelations(), Optional.of(false));
        }

        @SuppressWarnings("unchecked")
        private <T extends Node> T processSameType(T node)
        {
            return (T) process(node);
        }
    }
}
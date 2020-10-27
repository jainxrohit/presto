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
package com.facebook.presto.mv;

import au.com.bytecode.opencsv.CSVReader;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.Table;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class FetchMVQueryMetrics
{
//    String queryFile = "/Users/rohitism/work/mv/admetrics_queries/oct_06_12/mv_admetrics_queries.csv";
//    String outputFile = "/Users/rohitism/work/mv/admetrics_queries/oct_06_12/mv_admetrics_queries_metadata.txt";
//    String errorFile = "/Users/rohitism/work/mv/admetrics_queries/oct_06_12/mv_admetrics_queries_error.txt";

//    String queryFile = "/Users/rohitism/work/mv/admetrics_queries/oct_12/mv_admetrics_queries.csv";
//    String outputFile = "/Users/rohitism/work/mv/admetrics_queries/oct_12/mv_admetrics_queries_metadata.txt";
//    String errorFile = "/Users/rohitism/work/mv/admetrics_queries/oct_12/mv_admetrics_queries_error.txt";

    String queryFile = "/Users/rohitism/work/mv/admetrics_queries/oct_22/admetrics_queries.csv";
    String outputFile = "/Users/rohitism/work/mv/admetrics_queries/oct_22/admetrics_queries_filtered.txt";
    String errorFile = "/Users/rohitism/work/mv/admetrics_queries/oct_22/mv_admetrics_queries_error.txt";

    //Create a query to run shadow
    //create the
    Integer queryCount = 0;

    private Set<String> supportedTables = new HashSet<>();

    private SortedMap<String, Integer> metrics = new TreeMap<>();
    private SortedMap<String, Integer> dimensions = new TreeMap<>();
    private SqlParser sqlParser;
    private ParsingOptions parsingOptions;
    private PrintWriter errorWriter;

    public static void main(String[] args)
            throws IOException
    {
        FetchMVQueryMetrics fetchMVMetrics = new FetchMVQueryMetrics();
        fetchMVMetrics.init();
//        fetchMVMetrics.readAdMetricsQueries();
        fetchMVMetrics.deinit();
    }

    public void populateMVQueryIds()
    {

    }

    private void deinit()
    {
        errorWriter.close();
    }

    private void init()
            throws IOException
    {
        supportedTables.add("admetrics_output_nrt");
        sqlParser = new SqlParser();
        parsingOptions = ParsingOptions.builder().setDecimalLiteralTreatment(AS_DOUBLE).build();
        FileWriter fileWriter = new FileWriter(errorFile);
        errorWriter = new PrintWriter(fileWriter);
    }

    public void readAdMetricsQueries()
            throws IOException
    {
        long errorCounter = 0;
        try (CSVReader csvReader = new CSVReader(new FileReader(queryFile));) {
            String[] values;
            csvReader.readNext();
            while ((values = csvReader.readNext()) != null) {
                String queryId = values[0];
                String queryStr = values[1];
                Query query = null;
                try {
                    query = (Query) sqlParser.createStatement(queryStr, parsingOptions);
                }
                catch (ParsingException ex) {
                    errorWriter.println(String.format("[%d]Failed to parse query queryId: %s", ++errorCounter, queryId));
                    continue;
                }
                catch (Exception ex) {
                    errorWriter.println(String.format("[%d]Failed to parse query queryId: %s, ex: %s", ++errorCounter, queryId, ex));
                    continue;
                }
                processQuery(queryId, query);
            }
        }

        writeMetadata();
    }

    private void writeMetadata()
            throws IOException
    {
        FileWriter fileWriter = new FileWriter(outputFile);
        PrintWriter printWriter = new PrintWriter(fileWriter);

        printWriter.println("Total queries processed: " + queryCount);
        printWriter.println("Collected metrics are: ........");
        metrics.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).forEach(printWriter::println);

        printWriter.println("\nCollected dimensions are: ........");
        dimensions.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).forEach(printWriter::println);

        printWriter.close();
    }

    private void processQuery(String queryId, Query query)
    {
        requireNonNull(query, "query is null");
        queryCount++;
        QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
        processFrom(queryId, querySpecification.getFrom());
        processSelect(queryId, querySpecification.getSelect());

        StringBuffer filterKey = new StringBuffer();
        processWhere(queryId, querySpecification.getWhere(), filterKey);
        dimensions.put(filterKey.toString(), dimensions.getOrDefault(filterKey.toString(), 0) + 1);
    }

    private void processFrom(String queryId, Optional<Relation> from)
    {
        checkState(from.isPresent(), "From is not present for the queryId: " + queryId);
        Table table = (Table) from.get();
        String tableName = table.getName().getOriginalParts().get(0);
        checkState(supportedTables.contains(tableName), String.format("Table Name is not supported![queryId: %s, tableName: %s]", queryId, tableName));
    }

    private void processSelect(String queryId, Select select)
    {
        List<SelectItem> selectItems = select.getSelectItems();
        StringBuilder key = new StringBuilder("");
        for (SelectItem selectItem : selectItems) {
            checkState(selectItem instanceof SingleColumn, String.format("Found unsupported select Item: QueryId: %s, selectItem: %s", queryId, selectItem));
            SingleColumn singleColumn = (SingleColumn) selectItem;
            checkState(!singleColumn.getAlias().isPresent(), String.format("Found alias, queryId: %s, selectColumn: %s", queryId, singleColumn));
            Expression expression = ((SingleColumn) selectItem).getExpression();

            if (expression instanceof FunctionCall) {
                String functionName = ((FunctionCall) expression).getName().toString();
                checkState(functionName.equals("sum"), String.format("Unsupported ![queryId: %s, expression: %s]", queryId, expression));
                key.append(expression.toString()).append(", ");
//                metrics.put(expression.toString(), metrics.getOrDefault(expression.toString(), 0) + 1);
            }
            else if (expression instanceof Identifier) {
//                dimensions.put(expression.toString(), dimensions.getOrDefault(expression.toString(), 0) + 1);
                key.append(expression.toString()).append(", ");
            }
            else {
                System.err.println(String.format("Unsupported ![queryId: %s, expression: %s]", queryId, expression));
            }
        }

        metrics.put(key.toString(), metrics.getOrDefault(key.toString(), 0) + 1);
    }

    private void processWhere(String queryId, Optional<Expression> where, StringBuffer filterKey)
    {
        if (where.isPresent()) {
            Expression whereExpression = where.get();
            if (whereExpression instanceof LogicalBinaryExpression) {
                List<Expression> expressions = ExpressionUtils.extractPredicates((LogicalBinaryExpression) whereExpression);
                expressions.forEach(expression -> {
                    processFilterExpression(expression, filterKey);
                });
            }
            else {
                errorWriter.println("Failed to evaluate expression: " + whereExpression);
            }
        }
    }

    private void processFilterExpression(Expression expression, StringBuffer filterKey)
    {
        if (expression instanceof Literal) {
            return;
        }

        if (expression instanceof Identifier) {
            dimensions.put(expression.toString(), dimensions.getOrDefault(expression.toString(), 0) + 1);
            filterKey.append(expression.toString()).append(", ");
        }
        else {
            expression.getChildren().stream().filter(c -> c instanceof Expression).forEach(e -> processFilterExpression((Expression) e, filterKey));
        }
    }
}

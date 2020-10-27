package com.facebook.presto.mv;

import au.com.bytecode.opencsv.CSVReader;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Except;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.Node;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class FetchGoodQueries
{
    String queryFile = "/Users/rohitism/work/mv/admetrics_queries/oct_22/admetrics_queries.csv";
    String outputFile = "/Users/rohitism/work/mv/admetrics_queries/oct_22/admetrics_queries_filtered.txt";
    String errorFile = "/Users/rohitism/work/mv/admetrics_queries/oct_22/mv_admetrics_queries_error.txt";

    //Create a query to run shadow
    //create the
    Integer queryCount = 0;

    private Set<String> supportedTables = new HashSet<>();

    private HashSet<String> queryIds = new HashSet<>();
    private SqlParser sqlParser;
    private ParsingOptions parsingOptions;
    private PrintWriter errorWriter;
    private MVRegistry registry;
    private List<MVInfo> mvInfos;
    HashSet<String> columns;

    public static void main(String[] args)
            throws IOException
    {
        FetchGoodQueries fetchGoodQueries = new FetchGoodQueries();
        fetchGoodQueries.init();
        fetchGoodQueries.readAdMetricsQueries();
        fetchGoodQueries.deinit();
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
        registry = new MVRegistry();
        QualifiedObjectName qualifiedObjectName = new QualifiedObjectName("prism", "nrt", "admetrics_output_nrt");
        mvInfos = this.registry.getRegistry(qualifiedObjectName);
        populateMVColumns();
        supportedTables.add("admetrics_output_nrt");
        sqlParser = new SqlParser();
        parsingOptions = ParsingOptions.builder().setDecimalLiteralTreatment(AS_DOUBLE).build();
        FileWriter fileWriter = new FileWriter(errorFile);
        errorWriter = new PrintWriter(fileWriter);
    }

    private void populateMVColumns()
    {
        columns = new HashSet<>();
        columns.add("weight");
        columns.add("_price_to_value_ratio_mult_weight_");
        columns.add("_is_instream_ads_skippable_mult_weight_");
        columns.add("_paced_bid_mult_weight_");
        columns.add("_ads_good_click_mult_weight_");
        columns.add("_feature_story_carousel_opt_in_position_mult_weight_");
        columns.add("_unified_pixel_id_mult_weight_");
        columns.add("_advertiser_value_reserve_price_mult_weight_");
        columns.add("_is_instream_ad_pod_nonlive_mult_weight_");
        columns.add("_adfinder_eroh_online_calibration_multiplier_mult_weight_");
        columns.add("_lte_value_mult_weight_");
        columns.add("_is_from_related_ads_mult_weight_");
        columns.add("_browser_type_mult_weight_");
        columns.add("_dco_cta_count_mult_weight_");
        columns.add("_fb_story_ads_number_of_carousel_cards_before_opt_in_mult_weight_");
        columns.add("_dco_video_count_mult_weight_");
        columns.add("_dco_image_count_mult_weight_");
        columns.add("_targeting_delivery_signature_mult_weight_");
        columns.add("_is_related_ads_eligible_mult_weight_");
        columns.add("_external_legal_budget_usd_mult_weight_");
        columns.add("_user_dal_udv_bucket_id_mult_weight_");
        columns.add("_continuity_flags_mult_weight_");
        columns.add("_subsidy_coefficient_mult_weight_");
        columns.add("_carousel_smart_opt_in_prediction_mult_weight_");
        columns.add("_ig_acqs_survey_response_mult_weight_");
        columns.add("_clk_ecvr_mult_weight_");
        columns.add("_freedom_cali_multiplier_mult_weight_");
        columns.add("_freedom_cali_au_multiplier_mult_weight_");
        columns.add("_multifeed_ectr_mult_weight_");
        columns.add("_product_raw_pre_cali_ectr_pos0_mult_weight_");
        columns.add("_app_id_mult_weight_");
        columns.add("_max_bau_lookalike_score_mult_weight_");
        columns.add("realized_adevent_trait_type");
        columns.add("country");
        columns.add("page_type_int");
        columns.add("conversion_type");
        columns.add("ad_objective");
        columns.add("page_tab");
        columns.add("advertiser_bqrt_version");
        columns.add("ad_pivot_type");
        columns.add("mobile_os");
        columns.add("ad_attribution_event");
        columns.add("ad_event_primary_type");
        columns.add("ds");
        columns.add("time");
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
                try {
                    processQuery(queryId, query);
                    queryIds.add(queryId);
                }
                catch (Exception ex) {
                    //skip the query
                    continue;
                }
            }
        }

        writeMetadata();
    }

    private void writeMetadata()
            throws IOException
    {
        FileWriter fileWriter = new FileWriter(outputFile);
        PrintWriter printWriter = new PrintWriter(fileWriter);

        queryIds.forEach(printWriter::println);

        printWriter.close();
    }

    private void processQuery(String queryId, Query query)
            throws Exception
    {
        requireNonNull(query, "query is null");
        queryCount++;
        QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
        processFrom(queryId, querySpecification.getFrom());
        processSelect(queryId, querySpecification.getSelect());
        processWhere(queryId, querySpecification.getWhere());
    }

    private void processFrom(String queryId, Optional<Relation> from)
            throws Exception
    {
        checkState(from.isPresent(), "From is not present for the queryId: " + queryId);
        Table table = (Table) from.get();
        String tableName = table.getName().getOriginalParts().get(0);
        if (!supportedTables.contains(tableName)) {
            throw new Exception("Table is not good");
        }
    }

    private void processSelect(String queryId, Select select)
    {
        List<SelectItem> selectItems = select.getSelectItems();
        selectItems.forEach(selectItem -> {
            System.out.println(selectItems);
        });
    }

    private void processWhere(String queryId, Optional<Expression> where)
            throws Exception
    {
        if (where.isPresent()) {
            Expression whereExpression = where.get();
            if (whereExpression instanceof LogicalBinaryExpression) {
                List<Expression> expressions = ExpressionUtils.extractPredicates((LogicalBinaryExpression) whereExpression);
                for (Expression expression : expressions) {
                    processFilterExpression(expression);
                }
            }
            else {
                errorWriter.println("Failed to evaluate expression: " + whereExpression);
            }
        }
    }

    private void processFilterExpression(Expression expression)
            throws Exception
    {
        if (expression instanceof Literal) {
            return;
        }

        if (expression instanceof Identifier) {
            if (!columns.contains(expression.toString())) {
                throw new Exception("Not good!");
            }
        }
        else {
            for (Node c : expression.getChildren()) {
                if (c instanceof Expression) {
                    processFilterExpression((Expression) c);
                }
            }
        }
    }
}

package io.substrait.isthmus;

import com.google.protobuf.util.JsonFormat;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import net.hydromatic.tpcds.query.FixedQuery;
import net.hydromatic.tpcds.query.Query;
import org.apache.calcite.adapter.tpcds.TpcdsSchema;
import org.apache.calcite.util.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TpcdsQueryNoValidation extends PlanTestBase {

  /**
   * This test only validates that generating substrait plans for TPC-DS queries does not fail. As
   * of now this test does not validate correctness of the generated plan
   */
  @ParameterizedTest
  @ValueSource(ints = {7})
  public void tpcds(int query) throws Exception {
    SqlToSubstrait s = new SqlToSubstrait();
    String[] values = asString("tpcds/schema.sql").split(";");
    var creates = Arrays.stream(values).filter(t -> !t.trim().isBlank()).toList();
    var plan = s.execute(asString(String.format("tpcds/queries/%02d.sql", query)), creates);
    System.out.println(JsonFormat.printer().print(plan));
  }

  /*
   * Missing Functions
   * - Substring      -> {2, 15, 19, 23}
   * - Round          -> {8}
   * - ||             -> {5}
   * - STDDEV_SAMP    -> {17}
   * - UPPER          -> {24}
   * Missing features
   * - Unsupported subquery nested in Project    -> {9}
   * - Exception while converting
   *   SUM(`WS_EXT_SALES_PRICE`) * 100 / (SUM(SUM(`WS_EXT_SALES_PRICE`)) OVER (PARTITION B...  -> {12}
   * - Unable to find binding for call GROUPING($1)    -> {27}
   */

  /*
   * Fixes:
   * - use spark sql tpcds queries as reference to fix parse and generation errors
   *   - these are tpcds queries used in sparklinedata and spark_on_oracle projects
   *     - queries current as of mid 2021.
   *
   * - Parse
   *    - returns is keyword in Calcite SQL so change to rets
   *      - {5}
   *    - replace x days with interval 'x' day
   *      - {5, 16, 20, 21}
   *    - just exported define block, o replace with spark sql query
   *      - {12}
   * - Generator
   *   - doesn't handle pick from a date range, for example `date([YEAR]+"-08-01",[YEAR]+"-08-30",sales);`
   *     - {5, 20}
   *    - doesn't handle generation of ulist for example RC=ulist(random(1, rowcount("store_sales")/5,uniform),5);
   *      - picked constants from spark sql tpcds query set
   *      - {9, 18, 20}
   */

  @Test
  public void tpcdsAll() throws Exception {
    SqlToSubstrait s = new SqlToSubstrait();
    TpcdsSchema schema = new TpcdsSchema(1.0);
    // s.registerSchema("tpcds", schema);

    List<Integer> succeeded_queries = new ArrayList<>();
    List<Pair<Integer, Pair<String, String>>> failed_queries = new ArrayList<>();

    for (int i = 1; i < 100; i++) {

      final Query query = Query.of(i);
      // String sql = query.sql(new Random(0));
      String sql = asString(String.format("tpcds/queries/%02d.sql", i));

      try {
        var plan = s.execute(sql, "tpcds", schema);
        // System.out.println(JsonFormat.printer().print(plan));
        succeeded_queries.add(i);
      } catch (Exception e) {
        //
        failed_queries.add(Pair.of(i, Pair.of(sql, e.getMessage())));
      }
    }

    System.out.printf("Succeeded Queries:\n%s\n", succeeded_queries);
    System.out.printf(
            "Failed Queries:\n%s\n",
            failed_queries.stream().map(r -> r.left).collect(Collectors.toList()));

    System.out.println("-------------------------");
    System.out.println("Failed Queries Exceptions:");
    for (var failed_query : failed_queries) {

      var exceptionMsg =
              failed_query.right.right.length() > 100
                      ? failed_query.right.right.substring(0, 100) + "...."
                      : failed_query.right.right;

      System.out.printf("Failed for Query %d: Exception %s\n", failed_query.left, exceptionMsg);
    }

    System.out.println("-------------------------");
    System.out.println("Failed Queries SQLs:");
    for (var failed_query : failed_queries) {
      System.out.printf(
              "Failed for Query %d sql:\n%s\n", failed_query.left, failed_query.right.left);
    }
  }


  @Test
  public void tpcdsWriteAll() throws Exception {
    SqlToSubstrait s = new SqlToSubstrait();
    TpcdsSchema schema = new TpcdsSchema(1.0);
    // s.registerSchema("tpcds", schema);

    for (int i = 1; i < 100; i++) {

      final Query query = Query.of(i);
      // String sql = query.sql(new Random(0));
      String sql = FixedQuery.fixedQuerySQL(query, new Random(0));

      try(var writer = new FileWriter(String.format("./src/test/resources/tpcds/queries/%02d.sql", i))) {
          writer.append(sql);
          writer.flush();
      }
    }
  }

  private void testQuery(int i) throws Exception {
    SqlToSubstrait s = new SqlToSubstrait();
    TpcdsSchema schema = new TpcdsSchema(1.0);
    final Query query = Query.of(i);
    String sql = asString(String.format("tpcds/queries/%02d.sql", i));
    var plan = s.execute(sql, "tpcds", schema);
    // System.out.println(JsonFormat.printer().print(plan));
  }

  private void testQueryGen(int i) throws Exception {
    SqlToSubstrait s = new SqlToSubstrait();
    TpcdsSchema schema = new TpcdsSchema(1.0);
    final Query query = Query.of(i);
    String sql = FixedQuery.fixedQuerySQL(query, new Random(0));
    var plan = s.execute(sql, "tpcds", schema);
    // System.out.println(JsonFormat.printer().print(plan));
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 17, 24, 35})
  public void tpcdsMissingFunctions(int i) throws Exception {
    /* q2 : ROUND
      q17: STDDEV_SAMP
      q24: Unable to convert call UPPER(varchar<20>).
      q25: Unable to find binding for call MAX($4)
      q35: Unable to find binding for call MAX($3)
    */
    testQuery(i);
  }

  @ParameterizedTest
  @ValueSource(ints = {27})
  public void tpcdsMissingGroupingSupport(int i) throws Exception {
    /* q27 : Unable to find binding for call GROUPING($1)
     */
    testQuery(i);
  }

  @ParameterizedTest
  @ValueSource(ints = {4, 11, 14})
  public void tpcdsMissingLogicalUnion(int i) throws Exception {

    testQuery(i);
  }

  @ParameterizedTest
  @ValueSource(ints = {38})
  public void tpcdsMissingSetOps(int i) throws Exception {
    /*
     * q38: Unable to handle node: rel#350:LogicalIntersect.(input#0=LogicalIntersect#337,input#1=LogicalAggregate#348,all=false)
     */
    testQuery(i);
  }

  @ParameterizedTest
  @ValueSource(ints = {8, 19, 23, 30})
  public void tpcdsCacliteValidationErrors(int i) throws Exception {
    /*
     * q8: No match found for function signature SUBSTR(<CHARACTER>, <NUMERIC>, <NUMERIC>)
     * q19: From line 11, column 8 to line 11, column 25: No match found for function signature SUBSTR(<CHARACTER>, <NUMERIC>, <NUMERIC>)
     * q23: From line 9, column 12 to line 9, column 35: No match found for function signature SUBSTR(<CHARACTER>, <NUMERIC>, <NUMERIC>)
     * q30: From line 15, column 9 to line 15, column 26: Column 'C_LAST_REVIEW_DATE' not found in any table
     * q36: NeedToImplement error on -> RANK() OVER (PARTITION BY GROUPING(`I_CATEGORY`) + GROUPING(`I_CLASS`), CASE WHEN GROUPING(`I_CLASS`) = 0 THEN `I_CATEGORY` ELSE NULL END ORDER BY SUM(`SS_NET_PROFIT`) / SUM(`SS_EXT_SALES_PRICE`))
     */

    testQuery(i);
  }

  @ParameterizedTest
  @ValueSource(ints = {5, 15, 16, 20, 21, 32})
  public void tpcdsCacliteParseErrors(int i) throws Exception {
    /*
     * q5: issue with alias 'returns'. Is it keyword in Calcite SQL?
     * q15: From line 9, column 9 to line 9, column 26: No match found for function signature SUBSTR(<CHARACTER>, <NUMERIC>, <NUMERIC>)
     * q16: Encountered "days" at line 12, column 44 -> '(cast('2001-5-01' as date) + 60 days)'
     * q20: Encountered "days" at line 15, column 76. -> 'and (cast('date([YEAR]+"-01-01",[YEAR]+"-07-01",sales)' as date) + 30 days)'
     * q21: Encountered "days" at line 18, column 91.
     * q32: Encountered "days" at line 10, column 75.
     * q37: Encountered "days" at line 8, column 152.
     * q40: Encountered "days" at line 20, column 87.
     */
    testQuery(i);
  }

  @ParameterizedTest
  @ValueSource(ints = {9, 12, 18, 28, 33, 39})
  public void tpcdsQueryGenErrors(int i) throws Exception {
    /*
     * q9: cannot handle define RC=ulist(random(1, rowcount("store_sales")/5,uniform),5);
     * q12: DEFINE has no spaces at start of line
     * q18: define MONTH=ulist(random(1,12,uniform),6);
     * q28: define LISTPRICE=ulist(random(0, 190, uniform),6);
     * q33: cannot handle define COUNTY=random(1, rowcount("active_counties", "store"), uniform);
     * q39: define STATENUMBER=ulist(random(1, rowcount("active_states", "warehouse"), uniform),3);
     */
    testQuery(i);
  }

  @ParameterizedTest
  @ValueSource(
      ints = {
        43, 44, 45, 47, 49, 51, 53, 54, 56, 57, 60, 62, 63, 66, 67, 70, 71, 72, 74, 75, 76, 77, 78,
        79, 80, 82, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99
      })
  public void tpcdsErrorsToClassify(int i) throws Exception {
    testQuery(i);
  }

  @ParameterizedTest
  @ValueSource(
          ints = {44})
  public void tpcdsErrorsDay(int i) throws Exception {
    testQuery(i);
  }

  @Test
  public void experiment() throws Exception {
    SqlToSubstrait s = new SqlToSubstrait();
    TpcdsSchema schema = new TpcdsSchema(1.0);

    String sql = "";

    // substr -> substring
    sql = "select substring(ca_zip,1,5) ca_zip  FROM customer_address";

    // days
    sql = "select * from date_dim where d_date between cast('1998-08-01' as date)\n" +
            "                  and (cast('1998-08-01' as date) +  interval '14' day)";

    var plan = s.execute(sql, "tpcds", schema);
  }
}

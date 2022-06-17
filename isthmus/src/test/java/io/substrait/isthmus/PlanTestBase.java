package io.substrait.isthmus;

import static io.substrait.isthmus.SqlConverterBase.EXTENSION_COLLECTION;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import io.substrait.function.SimpleExtension;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import java.io.IOException;
import java.util.Arrays;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelBuilder;

public class PlanTestBase {
  protected final RelCreator creator = new RelCreator();
  protected final RelBuilder builder = creator.createRelBuilder();
  protected final RexBuilder rex = creator.rex();
  protected final RelDataTypeFactory type = creator.type();

  protected static final SimpleExtension.ExtensionCollection TEST_EXTENSION_COLLECTION;

  static {
    TEST_EXTENSION_COLLECTION = EXTENSION_COLLECTION.merge(SubstraitFunctionsForTests.testCollection);
  };

  public static String asString(String resource) throws IOException {
    return Resources.toString(Resources.getResource(resource), Charsets.UTF_8);
  }

  protected void assertProtoPlanRoundrip(String query) throws IOException, SqlParseException {
    assertProtoPlanRoundrip(query, new SqlToSubstrait());
  }

  protected void assertProtoPlanRoundrip(String query, SqlToSubstrait s)
      throws IOException, SqlParseException {
    String[] values = asString("tpch/schema.sql").split(";");
    var creates = Arrays.stream(values).filter(t -> !t.trim().isBlank()).toList();
    io.substrait.proto.Plan protoPlan1 = s.execute(query, creates);
    Plan plan = new ProtoPlanConverter(TEST_EXTENSION_COLLECTION).from(protoPlan1);
    io.substrait.proto.Plan protoPlan2 = new PlanProtoConverter().toProto(plan);
    assertEquals(protoPlan1, protoPlan2);
    var rootRels = s.sqlToRelNode(query, creates);
    assertEquals(rootRels.size(), plan.getRoots().size());
    for (int i = 0; i < rootRels.size(); i++) {
      var rootRel = SubstraitRelVisitor.convert(rootRels.get(i), TEST_EXTENSION_COLLECTION);
      assertEquals(rootRel.getRecordType(), plan.getRoots().get(i).getInput().getRecordType());
    }
  }

  protected void assertPlanRoundrip(Plan plan) throws IOException, SqlParseException {
    io.substrait.proto.Plan protoPlan1 = new PlanProtoConverter().toProto(plan);
    io.substrait.proto.Plan protoPlan2 =
        new PlanProtoConverter().toProto(new ProtoPlanConverter().from(protoPlan1));
    assertEquals(protoPlan1, protoPlan2);
  }
}

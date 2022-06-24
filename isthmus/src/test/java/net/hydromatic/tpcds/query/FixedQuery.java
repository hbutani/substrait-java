package net.hydromatic.tpcds.query;

import java.util.Map;
import java.util.Random;

public class FixedQuery {

  public static String fixedQuerySQL(Query q, Random random) {
    String s = q.template;
    for (Map.Entry<String, Query.Generator> entry : q.allArgs()) {
      final String key = entry.getKey();
      final Query.Generator generator = entry.getValue();
      String value = generator.generate(random);
      s = s.replaceAll("\\[" + key + "\\]", value);
    }

    /*
     * return only one query
     * Queries that return multiple: 14
     */
    var semiColonLoc = s.indexOf(';');
    return (semiColonLoc >= 0) ? s.substring(0, semiColonLoc) : s;
  }
}

package net.hydromatic.tpcds.query;

import java.util.Map;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class FixedQuery {

  interface BindSQL {
    String bind(String sql, String key, String value);
  }

  public static String fixedQuerySQL(Query q, Random random) {
    String s = q.template;

    BindSQL b = (String sql, String key, String value) -> sql.replaceAll("\\[" + key + "\\]", value);

    BiFunction<String, Map<String, String>, String> bindAll = (sql, valueMap) -> {
      for(var e : valueMap.entrySet()) {
        sql = b.bind(sql, e.getKey(), e.getValue());
      }
      return sql;
    };

    var valueMap0 =
            StreamSupport.stream(q.allArgs().spliterator(),false).
                    map( e -> Map.entry(e.getKey(), e.getValue().generate(random))).
                    collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            ;

    // recursively replace values, couple of times.
    // for example: [SALES_DATE may evaluate to `[YEAR]+"-01-31"`
    var valueMap1 = valueMap0.entrySet().stream().
            map( e -> Map.entry(e.getKey(), bindAll.apply(e.getValue(), valueMap0))).
            collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    var valueMap2 = valueMap1.entrySet().stream().
            map( e -> Map.entry(e.getKey(), bindAll.apply(e.getValue(), valueMap1))).
            collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));



    for (Map.Entry<String, String> entry : valueMap2.entrySet()) {
      s = b.bind(s, entry.getKey(), entry.getValue());
    }
    s = s.replaceAll("substr\\(", "substring(");

    /*
     * return only one query
     * Queries that return multiple: 14
     */
    var semiColonLoc = s.indexOf(';');
    return (semiColonLoc >= 0) ? s.substring(0, semiColonLoc) : s;
  }
}

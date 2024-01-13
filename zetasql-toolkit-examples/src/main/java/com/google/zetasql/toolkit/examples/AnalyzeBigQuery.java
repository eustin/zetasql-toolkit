package com.google.zetasql.toolkit.examples;

import com.google.zetasql.AnalyzerOptions;
import com.google.zetasql.resolvedast.ResolvedNodes;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedStatement;
import com.google.zetasql.toolkit.AnalyzedStatement;
import com.google.zetasql.toolkit.ZetaSQLToolkitAnalyzer;
import com.google.zetasql.toolkit.catalog.bigquery.BigQueryCatalog;
import com.google.zetasql.toolkit.options.BigQueryLanguageOptions;
import java.util.Iterator;
import java.util.Optional;

public class AnalyzeBigQuery {

  public static void main(String[] args) {
    String query =
        "WITH x AS (SELECT event_number FROM `simple-ci-cd.mock_nested_data.events`) SELECT * FROM x";

    BigQueryCatalog catalog = BigQueryCatalog.usingBigQueryAPI("simple-ci-cd");
    AnalyzerOptions options = new AnalyzerOptions();
    options.setLanguageOptions(BigQueryLanguageOptions.get());
    options.setPruneUnusedColumns(true);
    catalog.addAllTablesUsedInQuery(query, options);

    ZetaSQLToolkitAnalyzer analyzer = new ZetaSQLToolkitAnalyzer(options);
    Iterator<AnalyzedStatement> statementIterator = analyzer.analyzeStatements(query, catalog);
    AnalyzedStatement statement = statementIterator.next();
    Optional<ResolvedStatement> resolvedStatement = statement.getResolvedStatement();
    System.out.println(resolvedStatement);
    // Step 5: Consume the previous iterator and use the ResolvedStatements however you need
  //   statementIterator.forEachRemaining(statement ->
  //       statement.getResolvedStatement().ifPresent(System.out::println));
  }
}

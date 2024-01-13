package com.google.zetasql.toolkit.examples;

import com.google.zetasql.AnalyzerOptions;
import com.google.zetasql.resolvedast.ResolvedNodes;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedStatement;
import com.google.zetasql.toolkit.AnalyzedStatement;
import com.google.zetasql.toolkit.ZetaSQLToolkitAnalyzer;
import com.google.zetasql.toolkit.catalog.bigquery.BigQueryCatalog;
import com.google.zetasql.toolkit.options.BigQueryLanguageOptions;
import com.google.zetasql.toolkit.tools.lineage.ColumnEntity;
import com.google.zetasql.toolkit.tools.lineage.ColumnLineage;
import com.google.zetasql.toolkit.tools.lineage.ColumnLineageExtractor;

import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

public class AnalyzeBigQuery {
    private static void outputLineage(String query, Set<ColumnLineage> lineageEntries) {
    System.out.println("\nQuery:");
    System.out.println(query);
    System.out.println("\nLineage:");
    lineageEntries.forEach(lineage -> {
      System.out.printf("%s.%s\n", lineage.target.table, lineage.target.name);
      for (ColumnEntity parent : lineage.parents) {
        System.out.printf("\t\t<- %s.%s\n", parent.table, parent.name);
      }
    });
    System.out.println();
    System.out.println();
  }

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
    ResolvedStatement resolvedStatement = statementIterator.next().getResolvedStatement().get();
    Set<ColumnLineage> lineageEntries = ColumnLineageExtractor.extractColumnLevelLineage(resolvedStatement);
    outputLineage(query, lineageEntries);
    
  }
}

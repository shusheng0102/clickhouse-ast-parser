package com.clickhouse.parser;

import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.jdbc.parser.ClickHouseSqlParser;
import com.clickhouse.jdbc.parser.ClickHouseSqlStatement;
import com.clickhouse.parser.ast.FromClause;
import com.clickhouse.parser.ast.INode;
import com.clickhouse.parser.ast.SelectStatement;
import com.clickhouse.parser.ast.SelectUnionQuery;
import com.clickhouse.parser.ast.expr.ColumnExpr;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.clickhouse.jdbc.parser.ClickHouseSqlStatement.KEYWORD_FORMAT;

@Slf4j
public class TestAstParser {
    @Test
    public void testAst() {
        String sql = "SELECT DISTINCT t1.a,count(DISTINCT t1.b ) as tb FROM t1 RIGHT JOIN t2 ON t1.id = t2.id where t1.b > 10 LIMIT 1000";

        ClickHouseSqlStatement[] parse = ClickHouseSqlParser.parse(sql, new ClickHouseConfig());

        Optional<ClickHouseSqlStatement> first = Arrays.stream(parse).findFirst();
        for (ClickHouseSqlStatement s : parse){
            System.out.println(s.getOperationType().toString());
            String contentBetweenKeywords = s.getContentBetweenKeywords("SELECT", KEYWORD_FORMAT);
            System.out.println(contentBetweenKeywords);
            System.out.println(s.toString());
        }

        System.out.println("11111");
    }

    @Test
    public void testAstParser() {
        // parse SQL and generate its AST
        AstParser astParser = new AstParser();
        String sql1 = "ALTER TABLE my_db.my_tbl ADD COLUMN IF NOT EXISTS id Int64";
        Object parsedResult1 = astParser.parse(sql1);
        log.info(parsedResult1.toString());
        String sql2 = "ALTER TABLE my_db.my_tbl DROP PARTITION '2020-11-21'";
        Object parsedResult2 = astParser.parse(sql2);
        log.info(parsedResult2.toString());
         String sql = "SELECT t1.a,count(DISTINCT t1.b ) as tb FROM t1 RIGHT JOIN t2 ON t1.id = t2.id where t1.b > 10 LIMIT 1000";
        // String sql = "SELECT count(DISTINCT id ) from tableName";
        // String sql = "SELECT id  from tableName";
        AstParser astParser1 = new AstParser();
        SelectUnionQuery ast = (SelectUnionQuery) astParser1.parse(sql);
        List<SelectStatement> statements = ast.getStatements();
        SelectStatement selectStatement = statements.get(0);
        System.out.println(selectStatement.toString());
        boolean distinct = selectStatement.isDistinct();
        System.out.println(distinct);
    }

    @Test
    public void testReferredTablesDetector() {
        String sql = "SELECT DISTINCT t1.a,count(DISTINCT t1.b ),ab in (select b from tab1 where c=0) as tb FROM t1 RIGHT JOIN t2 ON t1.id = t2.id where t1.b > 10 LIMIT 1000";
        String sql1 = "\tSELECT DISTINCT --SELECT 子句 \n" +
                "\tcol1, col2, SUM(col3) AS total,toStartOfDay(toDateTime('2022-01-0112:00:00')), --列列表达式 \n" +
                "\tCASE WHEN col1 = 'foo' THEN col2 ELSE 'non-foo' END --CASE 表达式\n" +
                "\tFROM my_table t2\n" +
                "\tRIGHT JOIN t2 ON t1.id = t2.id\n" +
                "\tWHERE (col1 = 'foo' OR col2 = 'bar') AND col3 >10 --WHERE 子句\n" +
                "\tGROUP BY col1, col2 --GROUP BY 子句\n" +
                "\tHAVING SUM(col3) >100 --HAVING 子句\n" +
                "\tORDER BY total DESC, col1 ASC --ORDER BY 子句\n" +
                "\tLIMIT10 OFFSET0 --LIMIT 子句\n" +
                "\tFORMAT JSON --FORMAT 子句\n" +
                "\tSETTINGS max_memory_usage =100000000 --SETTINGS 子句\n" +
                "\tFINAL WITH TOTALS --WITH TOTALS修饰符;";
        AstParser astParser = new AstParser();
        Object ast = astParser.parse(sql1);
        SelectUnionQuery  selectUnionQuery= (SelectUnionQuery)ast;
        List<SelectStatement> statements = selectUnionQuery.getStatements();
        System.out.println(statements.size());
        SelectStatement selectStatement = statements.get(0);
        System.out.println(selectStatement.isDistinct());
        FromClause fromClause = selectStatement.getFromClause();
        List<ColumnExpr> exprs = selectStatement.getExprs();

        System.out.println("over");


    }




}

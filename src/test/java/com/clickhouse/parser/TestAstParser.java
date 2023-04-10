package com.clickhouse.parser;

import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.jdbc.parser.ClickHouseSqlParser;
import com.clickhouse.jdbc.parser.ClickHouseSqlStatement;
import com.clickhouse.parser.ast.DistributedTableInfoDetector;
import com.clickhouse.parser.ast.INode;
import com.clickhouse.parser.ast.SelectStatement;
import com.clickhouse.parser.ast.SelectUnionQuery;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;

import static com.clickhouse.jdbc.parser.ClickHouseSqlStatement.KEYWORD_FORMAT;

@Slf4j
public class TestAstParser {
    @Test
    public void testAst() {
        String sql = "SELECT t1.a,count(DISTINCT t1.b ) as tb FROM t1 RIGHT JOIN t2 ON t1.id = t2.id where t1.b > 10 LIMIT 1000";

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
        String sql = "SELECT t1.a FROM t1 RIGHT JOIN t2 ON t1.id = t2.id LIMIT 1000";
        AstParser astParser = new AstParser();
        Object ast = astParser.parse(sql);
        ReferredTablesDetector referredTablesDetector = new ReferredTablesDetector();
        List<String> tables = referredTablesDetector.searchTables((INode) ast);
        tables.parallelStream().forEach(table -> System.out.println(table));
    }

    @Test
    public void testDistributedTableInfoDetector() {
        String sql = "CREATE TABLE my_db.my_tbl (date Date, name String) Engine = Distributed('my_cluster', 'my_db', 'my_tbl_local', rand())";
        DistributedTableInfoDetector distributedTableInfoDetector = new DistributedTableInfoDetector();
        String clusterName = distributedTableInfoDetector.searchCluster(sql);
        log.info(clusterName);
        System.out.println(clusterName);
        long start = System.currentTimeMillis();
        String tableFullName = distributedTableInfoDetector.searchLocalTableFullName(sql);
        long end = System.currentTimeMillis();
        log.info(tableFullName);
        System.out.println(tableFullName);
        log.info("It takes " + (end - start) + " ms");
        System.out.println("It takes " + (end - start) + " ms");
    }

    @Test
    public void testDistributedTableInfoDetector2() {
        String sql = "CREATE TABLE mydb.mytb (uuid UUID DEFAULT generateUUIDv4(), cktime DateTime DEFAULT now() COMMENT 'c', openid String, username String, appid String, from_channel String, source_channel String, source String, regtime DateTime, brandid String, devicecode String, actiontime DateTime, ismingamelogin String, version String, platform String, project String, plat String, source_openid String COMMENT 'a', event Int16 COMMENT 'b') ENGINE = ReplicatedMergeTree('/clickhouse/mydb/mytb/{shard}', '{replica}') PARTITION BY toYYYYMM(cktime) ORDER BY (regtime, appid, openid) SETTINGS index_granularity = 8192";
        log.info("=========distributedTableInfoDetector");
        System.out.println("=========distributedTableInfoDetector");
        DistributedTableInfoDetector distributedTableInfoDetector = new DistributedTableInfoDetector();
        String clusterName = distributedTableInfoDetector.searchCluster(sql);
        log.info(clusterName);
        System.out.println(clusterName);
        long start = System.currentTimeMillis();
        String tableFullName = distributedTableInfoDetector.searchLocalTableFullName(sql);
        long end = System.currentTimeMillis();
        log.info(tableFullName);
        System.out.println(tableFullName);
        log.info("It takes " + (end - start) + " ms");
        System.out.println("It takes " + (end - start) + " ms");
    }

    @Test
    public void testDistributedTableInfoDetector3() {
        String sql = "CREATE TABLE my_db.my_tbl on cluster my_cluster Engine = Distributed('my_cluster', 'my_db', 'my_tbl_local', rand()) as my_db.my_tbl_local";
        DistributedTableInfoDetector distributedTableInfoDetector = new DistributedTableInfoDetector();
        String clusterName = distributedTableInfoDetector.searchCluster(sql);
        log.info(clusterName);
        long start = System.currentTimeMillis();
        String tableFullName = distributedTableInfoDetector.searchLocalTableFullName(sql);
        long end = System.currentTimeMillis();
        log.info(tableFullName);
        log.info("It takes " + (end - start) + " ms");
    }


}

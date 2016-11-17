package nablarch.common.idgenerator;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.sql.SQLException;

import nablarch.common.idgenerator.formatter.LpadFormatter;
import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.db.statement.SqlResultSet;
import nablarch.core.db.transaction.SimpleDbTransactionManager;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.TargetDb;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * {@link FastTableIdGenerator}のテスト。
 *
 * @author Hisaaki Sioiri
 */
@RunWith(DatabaseTestRunner.class)
public class FastTableIdGeneratorTest {

    @Rule
    public SystemRepositoryResource repositoryResource = new SystemRepositoryResource(
            "nablarch/common/idgenerator/FastTableIdGeneratorTest.xml");

    @BeforeClass
    public static void classSetup() throws SQLException {
        VariousDbTestHelper.createTable(SbnTbl.class);
    }

    /**
     * generatのテスト
     * 採番処理内でコミットされていることを確認。
     */
    @Test
    public void generate1() {

        VariousDbTestHelper.setUpTable(
                new SbnTbl("01", 100L),
                new SbnTbl("02", 0L),
                new SbnTbl("03", 200L),
                new SbnTbl("05", 999L));

        SimpleDbTransactionManager db = repositoryResource.getComponent("dbManager-default");
        db.beginTransaction();
        try {
            IdGenerator generator = repositoryResource.getComponent("idgenerator");

            assertThat(generator.generateId("02"), is("1"));

            // 採番処理内でコミットされているため、値がインクリメントされていることを確認
            db.rollbackTransaction();
            AppDbConnection connection = DbConnectionContext.getConnection();
            SqlPStatement statement = connection.prepareStatement("select * from sbn_tbl where id_col = '02'");
            SqlResultSet resultSet = statement.retrieve();
            assertThat(resultSet.size(), is(1));
            assertThat(resultSet.get(0)
                                .get("nocol")
                                .toString(), is("1"));

            db.commitTransaction();
        } finally {
            db.endTransaction();
        }
    }

    /**
     * generatのテスト
     * フォーマッターを指定した場合
     */
    @Test
    public void generate2() {
        VariousDbTestHelper.setUpTable(
                new SbnTbl("01", 100L),
                new SbnTbl("02", 0L),
                new SbnTbl("03", 200L),
                new SbnTbl("05", 999L));

        SimpleDbTransactionManager db = repositoryResource.getComponent("dbManager-default");
        db.beginTransaction();
        try {
            IdGenerator generator = repositoryResource.getComponent("idgenerator");

            assertThat(generator.generateId("05", new LpadFormatter(5, '0')), is("01000"));
            assertThat(generator.generateId("05", null), is("1001"));

            // 採番処理内でコミットされているため、値がインクリメントされていることを確認
            db.rollbackTransaction();
            AppDbConnection connection = DbConnectionContext.getConnection();
            SqlPStatement statement = connection.prepareStatement("select * from sbn_tbl where id_col = '05'");
            SqlResultSet resultSet = statement.retrieve();
            assertThat(resultSet.size(), is(1));
            assertThat(resultSet.get(0)
                                .get("nocol")
                                .toString(), is("1001"));

            db.commitTransaction();
        } finally {
            db.endTransaction();
        }
    }

    /**
     * generatのテスト
     * 採番処理でエラーが発生した場合
     */
    @Test
    @TargetDb(exclude = {TargetDb.Db.POSTGRE_SQL, TargetDb.Db.DB2})
    public void generate3() {
        VariousDbTestHelper.setUpTable(
                new SbnTbl("01", 99999L),
                new SbnTbl("02", 0L),
                new SbnTbl("03", 200L),
                new SbnTbl("05", 9L));

        SimpleDbTransactionManager db = repositoryResource.getComponent("dbManager-default");
        db.beginTransaction();
        try {
            IdGenerator generator = repositoryResource.getComponent("idgenerator");

            generator.generateId("01");
            db.commitTransaction();
            fail();
        } catch (Exception e) {
            assertThat(e.getMessage(), is("failed in generation of id. id = 01"));
        } finally {
            // 採番処理内でエラーが発生しているため、値はインクリメントされていないこと。
            db.rollbackTransaction();
            AppDbConnection connection = DbConnectionContext.getConnection();
            SqlPStatement statement = connection.prepareStatement("select * from sbn_tbl where id_col = '01'");
            SqlResultSet resultSet = statement.retrieve();
            assertThat(resultSet.size(), is(1));
            assertThat(resultSet.get(0)
                                .get("nocol")
                                .toString(), is("99999"));
            db.endTransaction();
        }
    }

    /**
     * generatのテスト
     * 既に開始されているトランザクションで採番処理を実施した場合。
     * 例外が発生すること
     */
    @Test(expected = RuntimeException.class)
    public void generate4() {
        VariousDbTestHelper.setUpTable(
                new SbnTbl("01", 100L),
                new SbnTbl("02", 0L),
                new SbnTbl("03", 200L),
                new SbnTbl("05", 999L));

        SimpleDbTransactionManager db = repositoryResource.getComponent("dbManager");

        db.beginTransaction();
        try {
            IdGenerator generator = repositoryResource.getComponent("idgenerator");

            generator.generateId("02");

            fail("ここはとおらない。");
        } finally {
            db.endTransaction();
        }
    }

    /**
     * generatのテスト
     * トランザクション名を指定しない場合は、採番クラスが自動でトランザクション名を指定してトランザクションを制御すること。
     * 例外が発生すること
     */
    @Test
    public void generate5() {
        VariousDbTestHelper.setUpTable(
                new SbnTbl("01", 100L),
                new SbnTbl("02", 0L),
                new SbnTbl("03", 200L),
                new SbnTbl("05", 999L));

        // 既定のトランザクション名でトランザクションを開始
        SimpleDbTransactionManager db = repositoryResource.getComponent("dbManager-default");
        db.beginTransaction();
        try {
            // トランザクション名が設定されていない採番クラスを取得
            IdGenerator generator = repositoryResource.getComponent("idgenerator2");

            //　採番処理を実行
            generator.generateId("02");

            // エラーとならずに想定どおり採番されることを確認
            AppDbConnection connection = DbConnectionContext.getConnection();
            SqlPStatement statement = connection.prepareStatement("select * from sbn_tbl where id_col = '02'");
            SqlResultSet resultSet = statement.retrieve();
            assertThat(resultSet.size(), is(1));
            assertThat(resultSet.get(0)
                                .get("nocol")
                                .toString(), is("1"));

        } finally {
            db.endTransaction();
        }
    }
}

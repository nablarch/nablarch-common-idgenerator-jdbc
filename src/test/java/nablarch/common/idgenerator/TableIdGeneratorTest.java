package nablarch.common.idgenerator;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import nablarch.common.idgenerator.formatter.LpadFormatter;
import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.db.statement.SqlResultSet;
import nablarch.core.db.transaction.SimpleDbTransactionManager;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * {@link TableIdGenerator}のテストクラス。
 *
 */
@RunWith(DatabaseTestRunner.class)
public class TableIdGeneratorTest {

    @Rule
    public SystemRepositoryResource repositoryResource = new SystemRepositoryResource(
            "nablarch/common/idgenerator/TableIdGeneratorTest.xml");


    @BeforeClass
    public static void classSetup() {
        VariousDbTestHelper.createTable(SbnTbl.class);
    }

    /**
     * generateのテスト
     * DBリソース名はデフォルト
     * DBの値がインクリメントされること
     */
    @Test
    public void generate1() throws Exception {
        VariousDbTestHelper.setUpTable(
                new SbnTbl("01", 100L),
                new SbnTbl("02", 0L),
                new SbnTbl("03", 200L),
                new SbnTbl("05", 999L));

        SimpleDbTransactionManager db = repositoryResource.getComponent("dbManager-default");
        db.beginTransaction();
        String id;
        try {
            TableIdGenerator generator = repositoryResource.getComponent("idgenerator");
            generator.initialize();
            id = generator.generateId("02");

            // コミット前は、値がインクリメントされていないこと
            // テーブルが更新されたことを確認
            SbnTbl sbnTbl = VariousDbTestHelper.findById(SbnTbl.class, "02");
            assertThat(sbnTbl.noCol, is(0L));

            db.commitTransaction();
        } finally {
            db.endTransaction();
        }

        assertEquals("1", id);

        // テーブルが更新されたことを確認
        SbnTbl sbnTbl = VariousDbTestHelper.findById(SbnTbl.class, "02");
        assertThat(sbnTbl.noCol, is(1L));
    }

    /**
     * generateのテスト
     * DBリソース名はデフォルト以外を指定
     */
    @Test
    public void generate2() throws Exception {
        VariousDbTestHelper.setUpTable(
                new SbnTbl("01", 100L),
                new SbnTbl("02", 0L),
                new SbnTbl("03", 200L),
                new SbnTbl("05", 999L));

        SimpleDbTransactionManager db = repositoryResource.getComponent("dbManager-default");
        SimpleDbTransactionManager dbGenerator = repositoryResource.getComponent("dbManager");
        db.beginTransaction();
        dbGenerator.beginTransaction();
        String id;
        try {
            TableIdGenerator generator = repositoryResource.getComponent("idgenerator2");
            generator.initialize();
            id = generator.generateId("03");

            // デフォルトトランザクションで参照するとDB値はインクリメントされていないこと。
            AppDbConnection dbConnection = DbConnectionContext.getConnection();
            SqlPStatement pStatement = dbConnection.prepareStatement("select * from sbn_tbl where id_col = '03'");
            SqlResultSet set = pStatement.retrieve();
            assertThat(set.get(0)
                          .getString("NO_COL"), is("200"));

            dbGenerator.commitTransaction();
        } finally {
            db.endTransaction();
            dbGenerator.endTransaction();
        }

        assertEquals("201", id);

        // テーブルが更新されたことを確認
        SbnTbl sbnTbl = VariousDbTestHelper.findById(SbnTbl.class, "03");
        assertThat(sbnTbl.noCol, is(201L));

    }

    /**
     * generateのテスト
     * フォーマッターを指定した場合
     */
    @Test
    public void generate3() throws Exception {
        VariousDbTestHelper.setUpTable(
                new SbnTbl("01", 100L),
                new SbnTbl("02", 0L),
                new SbnTbl("03", 200L),
                new SbnTbl("05", 999L));

        SimpleDbTransactionManager db = repositoryResource.getComponent("dbManager-default");
        db.beginTransaction();
        String formatId;
        String notFormatId;
        try {
            TableIdGenerator generator = repositoryResource.getComponent("idgenerator");
            generator.initialize();
            formatId = generator.generateId("05", new LpadFormatter(10, 'a'));
            notFormatId = generator.generateId("05", null);
            db.commitTransaction();
        } finally {
            db.endTransaction();
        }

        assertThat(formatId, is("aaaaaa1000"));
        assertThat(notFormatId, is("1001"));

        // テーブルが更新されたことを確認
        SbnTbl sbnTbl = VariousDbTestHelper.findById(SbnTbl.class, "05");
        assertThat(sbnTbl.noCol, is(1001L));
    }

    /**
     * generateのテスト
     * 指定されたIDに対応するデータが存在しない場合
     */
    @Test
    public void generate4() throws Exception {
        VariousDbTestHelper.setUpTable(
                new SbnTbl("01", 100L),
                new SbnTbl("02", 0L),
                new SbnTbl("03", 200L),
                new SbnTbl("05", 999L));

        SimpleDbTransactionManager db = repositoryResource.getComponent("dbManager-default");
        db.beginTransaction();
        try {
            TableIdGenerator generator = repositoryResource.getComponent("idgenerator");
            generator.initialize();
            generator.generateId("04", new LpadFormatter(10, 'a'));
            db.commitTransaction();

            fail("");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), is("id was not found. id:04"));
        } finally {
            db.endTransaction();
        }
    }
}


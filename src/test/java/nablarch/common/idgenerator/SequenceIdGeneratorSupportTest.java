package nablarch.common.idgenerator;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import nablarch.core.db.transaction.SimpleDbTransactionManager;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;


/**
 * SequenceIdGeneratorSupportのテストクラス。
 *
 * @author Naoki Yamamoto
 */
@RunWith(DatabaseTestRunner.class)
public class SequenceIdGeneratorSupportTest {

    @Rule
    public SystemRepositoryResource repositoryResource = new SystemRepositoryResource(
            "nablarch/common/idgenerator/SequenceIdGeneratorSupportTest.xml");

    @BeforeClass
    public static void setUpClass() throws Exception {
        VariousDbTestHelper.createTable(SequenceTest.class);
    }

    @Before
    public void testSetup() {
        VariousDbTestHelper.setUpTable(
                new SequenceTest("URI_ID_SEQ", "1"),
                new SequenceTest("NYK_ID_SEQ", "2"),
                new SequenceTest("CST_ID_SEQ", "3"),
                new SequenceTest("SKY_ID_SEQ", "4"),
                new SequenceTest("SAMPLE", "5"));
    }

    /**
     * generateIdのテスト。
     * デフォルトのデータベースリソース名を使用する場合
     *
     * @throws Exception
     */
    @Test
    public void testGenerateId1() throws Exception {
        SimpleDbTransactionManager db = repositoryResource.getComponent("dbManager-default");
        db.beginTransaction();
        try {
            TestSequenceIdGenerator generator = repositoryResource.getComponent("TestSequence");

            // ID->シーケンスオブジェクト変換を行って採番
            assertThat(generator.generateId("01"), is("1"));

            // formatterを指定
            assertThat(generator.generateId("01", new IdFormatter() {
                public String format(String id, String no) {
                    return "20100824" + no;
                }
            }), is("201008241"));

            // IDをシーケンスオブジェクトとして採番
            assertThat(generator.generateId("SAMPLE"), is("5"));

        } finally {
            db.endTransaction();
        }

    }

    /**
     * generateIdのテスト。
     * トランザクションリソース名を指定した場合
     *
     * @throws Exception
     */
    @Test
    public void testGenerateId2() throws Exception {
        SimpleDbTransactionManager db = repositoryResource.getComponent("dbManager");
        db.beginTransaction();
        try {
            TestSequenceIdGenerator generator = repositoryResource.getComponent("TestSequence2");
            // ID->シーケンスオブジェクト変換を行って採番
            assertThat(generator.generateId("01"), is("1"));

            // IDをシーケンスオブジェクトとして採番
            assertThat(generator.generateId("SAMPLE"), is("5"));
        } finally {
            db.endTransaction();
        }

    }

    /**
     * generateIdのテスト。
     * 存在しないIDを指定した場合。
     *
     * @throws Exception
     */
    @Test
    public void testGenerateIdError() throws Exception {
        SimpleDbTransactionManager db = repositoryResource.getComponent("dbManager");
        db.beginTransaction();
        try {
            TestSequenceIdGenerator generator = repositoryResource.getComponent("TestSequence");
            generator.generateId("存在しないID");

            fail("ここはとおらない。");
        } catch (Exception e) {
            assertThat(e.getMessage(), is("id was not found. id:存在しないID"));
        } finally {
            db.endTransaction();
        }

    }

    /**
     * setIdTableのテスト。
     *
     * @throws Exception
     */
    @Test
    public void testSetIdTable() throws Exception {
        TestSequenceIdGenerator generator = new TestSequenceIdGenerator();
        Map<String, String> table = new HashMap<String, String>();
        // 正常データのみの場合
        table.put("01", "1234");
        table.put("02", "5432");
        // ここではエラーは発生しない。
        generator.setIdTable(table);

        // 不正なシーケンス名を設定
        String sql = "(select table_name from all_tables where rownum <= 1)";
        table.put("03", sql);
        try {
            generator.setIdTable(table);
            fail("ここはとおらない。");
        } catch (Exception e) {
            assertThat(e.getMessage(), is("invalid sequence name. id = 03, sequence name = " + sql));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIdTableIsNull() {
        TestSequenceIdGenerator generator = new TestSequenceIdGenerator();
        generator.generateId("id");
    }

    @Entity
    @Table(name = "SEQUENCE_TEST")
    public static class SequenceTest {

        public SequenceTest() {
        }

        public SequenceTest(String sequenceName, String generateId) {
            this.sequenceName = sequenceName;
            this.generateId = generateId;
        }

        @Id
        @Column(name = "SEQUENCE_NAME", length = 128, nullable = false)
        public String sequenceName;

        @Column(name = "GENERATE_ID", length = 10, nullable = false)
        public String generateId;
    }
}

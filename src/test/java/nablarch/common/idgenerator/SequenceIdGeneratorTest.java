package nablarch.common.idgenerator;

import nablarch.common.idgenerator.SequenceIdGenerator.SequenceGeneratorFailedException;
import nablarch.core.db.DbAccessException;
import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.db.statement.ResultSetIterator;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.TargetDb;
import nablarch.test.support.log.app.OnMemoryLogWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * {@link SequenceIdGenerator}のテストクラス。
 *
 * @author hisaaki shioiri
 */
@RunWith(DatabaseTestRunner.class)
@TargetDb(exclude = TargetDb.Db.SQL_SERVER)
public class SequenceIdGeneratorTest {

    /** テストで使用するコネクション名 */
    private static final String CONNECTION_NAME = "connection name";

    @ClassRule
    public static SystemRepositoryResource repositoryResource = new SystemRepositoryResource("db-default.xml");

    /** テスト対象 */
    private SequenceIdGenerator sut = new SequenceIdGenerator();

    @Before
    public void setUp() throws Exception {
        final ConnectionFactory factory = repositoryResource.getComponentByType(ConnectionFactory.class);
        final TransactionManagerConnection connection = factory.getConnection(CONNECTION_NAME);
        DbConnectionContext.setConnection(CONNECTION_NAME, connection);
        sut.setDbTransactionName(CONNECTION_NAME);

        createSequence(connection, "SEQ1", "SEQ2");
        OnMemoryLogWriter.clear();
    }

    private void createSequence(TransactionManagerConnection connection, String... sequences) {
        for (String sequence : sequences) {
            final SqlPStatement drop = connection.prepareStatement("DROP SEQUENCE " + sequence);
            try {
                drop.execute();
            } catch (Exception ignore) {
                connection.rollback();
                ignore.printStackTrace();
            }
            drop.close();
            final SqlPStatement create = connection.prepareStatement("create SEQUENCE " + sequence);
            create.execute();
            create.close();
            connection.commit();
        }
    }

    @After
    public void tearDown() throws Exception {
        try {
            final TransactionManagerConnection connection = DbConnectionContext.getTransactionManagerConnection(
                    CONNECTION_NAME);
            connection.terminate();
        } finally {
            DbConnectionContext.removeConnection(CONNECTION_NAME);
        }
    }

    /**
     * シーケンスを用いた採番ができること。
     */
    @Test
    public void generateId() throws Exception {
        final String first = sut.generateId("SEQ1");
        final String second = sut.generateId("SEQ1");

        assertThat("2回目の呼び出しで1回目から値がインクリメントされていること",
                Integer.parseInt(second),
                is(Integer.parseInt(first) + 1));
    }

    /**
     * シーケンスを用いた採番が出来、フォーマットされた値が返却されること。
     */
    @Test
    public void generateId_withFormatter() throws Exception {
        final IdFormatter formatter = new IdFormatter() {

            @Override
            public String format(String id, String no) {
                return no + '_';
            }
        };
        final String value = sut.generateId("SEQ1", formatter);
        assertThat("最後の文字が「_」である", value.matches("\\d+_"), is(true));

        final String first = sut.generateId("SEQ2", formatter);
        final String second = sut.generateId("SEQ2", formatter);

        assertThat("2回目の呼び出しで1回目から値がインクリメントされていること",
                Integer.parseInt(second.substring(0, second.length() - 1)),
                is(Integer.parseInt(first.substring(0, second.length() - 1)) + 1));

    }

    /**
     * シーケンス採番のSQL実行でレコードが戻されない場合、
     * {@link SequenceIdGenerator.SequenceGeneratorFailedException}が送出されること。
     */
    @Test
    public void generateId_recordNotFound() throws Exception {
        // 採番処理中にSQLExceptionを送出するモックオブジェクトを設定する。
        try (final MockedStatic<DbConnectionContext> mocked = mockStatic(DbConnectionContext.class)) {
            final TransactionManagerConnection connection = mock(TransactionManagerConnection.class, RETURNS_DEEP_STUBS);
            mocked.when(() -> DbConnectionContext.getTransactionManagerConnection(anyString())).thenReturn(connection);
            
            final ResultSetIterator rs = Mockito.mock(ResultSetIterator.class);
            when(connection.prepareStatement(any()).executeQuery()).thenReturn(rs);
            
            when(rs.next()).thenReturn(false);

            try {
                sut.generateId("SEQ2");
                fail("ここは通らない");
            } catch (SequenceGeneratorFailedException e) {
                assertThat(e.getMessage(), is("failed to get next value from sequence. sequence name=[SEQ2]"));
            }
        }
    }

    /**
     * シーケンス採番時に使用する{@link ResultSet}のクローズ処理に失敗する場合、
     * ワーニングログが出力されること。
     */
    @Test
    public void generatedId_ResultSetCloseError() throws Exception {
        // 採番処理中にSQLExceptionを送出するモックオブジェクトを設定する。
        try (final MockedStatic<DbConnectionContext> mocked = mockStatic(DbConnectionContext.class)) {
            final TransactionManagerConnection connection = mock(TransactionManagerConnection.class, RETURNS_DEEP_STUBS);
            mocked.when(() -> DbConnectionContext.getTransactionManagerConnection(anyString())).thenReturn(connection);

            final ResultSetIterator rs = Mockito.mock(ResultSetIterator.class);
            when(connection.prepareStatement(any()).executeQuery()).thenReturn(rs);

            when(rs.next()).thenReturn(true);
            when(rs.getLong(anyInt())).thenReturn(1L);
            doThrow(new DbAccessException("db close error", new SQLException("error")))
                    .when(rs).close();

            final String id = sut.generateId("SEQ2");
            assertThat(id, is("1"));

            OnMemoryLogWriter.assertLogContains("writer.memory", "failed to ResultSetIterator#close");
        }
    }
}


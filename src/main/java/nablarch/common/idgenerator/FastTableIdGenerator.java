package nablarch.common.idgenerator;

import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.transaction.SimpleDbTransactionExecutor;
import nablarch.core.db.transaction.SimpleDbTransactionManager;
import nablarch.core.repository.initialization.Initializable;
import nablarch.core.transaction.TransactionContext;

/**
 * 採番用のテーブルを使用して高速に採番を行うクラス。<br>
 * 採番用テーブルを使用して採番処理を行い、トランザクションのコミット処理を行う。
 *
 * @author Hisaaki Sioiri
 * @see nablarch.common.idgenerator.TableIdGenerator
 */
public class FastTableIdGenerator implements IdGenerator, Initializable {

    /** データベースマネージャ */
    private SimpleDbTransactionManager dbTransactionManager;

    /** テーブル採番クラス */
    private TableIdGenerator tableIdGenerator;

    /** 採番テーブル物理名 */
    private String tableName;

    /** 採番テーブルのIDカラム物理名 */
    private String idColumnName;

    /** 採番テーブルのNOカラム物理名 */
    private String noColumnName;


    /** {@inheritDoc} */
    public String generateId(String id) {
        return generateId(id, null);
    }

    /** {@inheritDoc}* */
    public String generateId(final String id, final IdFormatter formatter) {
        try {
            return new SimpleDbTransactionExecutor<String>(
                    dbTransactionManager) {
                @Override
                public String execute(AppDbConnection connection) {
                    return tableIdGenerator.generateId(id, formatter);
                }
            }
            .doTransaction();
        } catch (RuntimeException e) {
            throw new RuntimeException(
                    "failed in generation of id. id = " + id, e);
        } catch (Error e) {
            throw new RuntimeException(
                    "failed in generation of id. id = " + id, e);
        }
    }

    /**
     * データベーストランザクションマネージャを設定する。。
     *
     * @param dbTransactionManager データベーストランザクションマネージャ
     */
    public void setDbTransactionManager(
            SimpleDbTransactionManager dbTransactionManager) {
        this.dbTransactionManager = dbTransactionManager;
    }

    /**
     * 採番テーブル物理名を設定する。
     *
     * @param tableName テーブル名
     */
    public void setTableName(String tableName) {
        this.tableName = tableName.toUpperCase();
    }

    /**
     * IDカラム物理名を設定する。
     *
     * @param idColumnName IDカラム名
     */
    public void setIdColumnName(String idColumnName) {
        this.idColumnName = idColumnName.toUpperCase();
    }

    /**
     * NOカラム物理名を設定する。
     *
     * @param noColumnName NOカラム名
     */
    public void setNoColumnName(String noColumnName) {
        this.noColumnName = noColumnName.toUpperCase();
    }

    /**
     * 初期化処理。<br>
     * 下記処理を行う。
     * <pre>
     * データベーストランザクション名の設定
     * {@link nablarch.common.idgenerator.TableIdGenerator}の初期化処理
     * </pre>
     */
    public void initialize() {

        if (TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY.equals(
                dbTransactionManager.getDbTransactionName())) {
            // トランザクション名が、既定の名前の場合には、デフォルトのトランザクション名を設定する。
            dbTransactionManager.setDbTransactionName(
                    this.getClass().getName());
        }

        // テーブル採番の初期化
        tableIdGenerator = new TableIdGenerator();
        tableIdGenerator.setTableName(this.tableName);
        tableIdGenerator.setIdColumnName(this.idColumnName);
        tableIdGenerator.setNoColumnName(this.noColumnName);
        tableIdGenerator.setDbTransactionName(
                dbTransactionManager.getDbTransactionName());
        tableIdGenerator.initialize();
    }
}


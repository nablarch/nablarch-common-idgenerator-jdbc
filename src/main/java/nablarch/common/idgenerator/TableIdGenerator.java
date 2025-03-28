package nablarch.common.idgenerator;

import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.db.statement.SqlResultSet;
import nablarch.core.repository.initialization.Initializable;
import nablarch.core.transaction.TransactionContext;

/**
 * 採番用のテーブルを使用して、連番の採番を行うクラス。<br>
 * <br>
 * このクラスでは、業務トランザクションを使用して採番処理を行い、トランザクションのコミット処理は行わずに採番した値を返却する。<br>
 * このため、業務アプリケーションの処理が確定されるまでコミットは行われないため、抜け番を発生させずに採番を行うことができる。<br>
 * ただし、業務アプリケーションが確定されるまではロックが保有されるため、その他の業務処理でロック待機が発生し著しく性能を劣化させる可能性があるため注意が必要である。<br>
 * <br>
 * また、本クラスはリポジトリの機能を用いて初期化することを想定しているので、コンポーネント設定ファイルに初期化の設定を行うこと。
 *
 * @author Hisaaki Sioiri
 */
public class TableIdGenerator implements IdGenerator, Initializable {

    /** 採番テーブル物理名 */
    private String tableName;

    /** 採番テーブルのIDカラム物理名 */
    private String idColumnName;

    /** 採番テーブルのNOカラム物理名 */
    private String noColumnName;

    /** データベーストランザクション名 */
    private String dbTransactionName = TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY;

    /** 更新用SQL */
    private String updateSql;

    /** 取得用SQL */
    private String selectSql;

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
     * データベースリソース名を設定する。
     *
     * @param dbTransactionName データベースリソース名
     */
    public void setDbTransactionName(String dbTransactionName) {
        this.dbTransactionName = dbTransactionName;
    }

    /**
     * 初期化処理を行う。
     *
     * 採番テーブル更新用、取得用のSQL文を組み立てる。
     */
    public void initialize() {
        String tmpUpdateSql = "  UPDATE $TABLE_NAME$ "
                + "   SET $NO$ = $NO$ + 1 "
                + " WHERE $ID$ = ?";

        updateSql = tmpUpdateSql.replace("$TABLE_NAME$", tableName)
                .replace("$NO$", noColumnName)
                .replace("$ID$", idColumnName);

        String tmpSelectSql = "  SELECT $NO$ "
                + "  FROM $TABLE_NAME$ "
                + " WHERE $ID$ = ?";
        selectSql = tmpSelectSql.replace("$TABLE_NAME$", tableName)
                .replace("$NO$", noColumnName)
                .replace("$ID$", idColumnName);
    }

    /** {@inheritDoc} */
    public String generateId(String id) {
        return generate(id);
    }

    /** {@inheritDoc} */
    public String generateId(String id, IdFormatter formatter) {
        String no = generate(id);
        if (formatter == null) {
            return no;
        }
        return formatter.format(id, no);
    }

    /**
     * IDに紐付くデータのインクリメント処理と対象IDのロック処理を行う。
     *
     * @param id 採番対象を識別するためのID
     * @return 採番したID
     */
    private String generate(String id) {
        AppDbConnection connection = DbConnectionContext.getConnection(dbTransactionName);
        // インクリメント、ロック
        SqlPStatement update = connection.prepareStatement(updateSql);
        update.setString(1, id);
        if (update.executeUpdate() != 1) {
            // 更新対象が存在しない場合は、エラー
            throw new IllegalStateException(String.format("id was not found. id:%s", id));
        }

        // インクリメントしたIDを取得し返却する。
        SqlPStatement select = connection.prepareStatement(selectSql);
        select.setString(1, id);
        SqlResultSet rs = select.retrieve(1, 1);
        return rs.get(0).getString(noColumnName);
    }
}


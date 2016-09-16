package nablarch.common.idgenerator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.db.statement.SqlResultSet;
import nablarch.core.transaction.TransactionContext;
import nablarch.core.util.annotation.Published;


/**
 * シーケンスを使用した採番処理をサポートするクラス。<br>
 * サブクラスでは、{@link SequenceIdGeneratorSupport#createSql(String)}を実装し、
 * シーケンスオブジェクトを使用した採番用SQLを生成すること。<br>
 *
 * @author Hisaaki Sioiri
 *
 * @deprecated 本実装は、{@link nablarch.core.db.dialect.Dialect}を使用してシーケンス採番を行う{@link SequenceIdGenerator}に置き換わりました。
 */
@Deprecated
public abstract class SequenceIdGeneratorSupport implements IdGenerator {

    /** 採番対称を識別するIDとシーケンス名を紐付けるテーブル */
    private Map<String, String> idTable;
    /** データベースリソース名 */
    private String dbTransactionName = TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY;
    /** SQLキャッシュ */
    private final Map<String, String> sqlCache = new ConcurrentHashMap<String, String>();

    /**
     * コンストラクタ。
     */
    @Published(tag = "architect")
    protected SequenceIdGeneratorSupport() {
    }

    /**
     * 引数で指定された採番対象ID内でユニークなIDを採番する。
     *
     * 採番対象IDに対応するシーケンスオブジェクト名を設定ファイル({@link #setIdTable(java.util.Map)}に設定された情報)
     * から取得し、採番処理を行う。
     *
     * {@link #setIdTable(java.util.Map)}に設定されたIDとシーケンスの紐付けテーブルに、
     * 指定されたIDが存在しない場合は、{@link IllegalArgumentException}を送出する。
     *
     * @param id 採番対象を識別するID
     * @return 採番対象ID内でユニークな採番結果のID
     */
    @Override
    public String generateId(String id) {
        return generateId(id, null);
    }

    /**
     * {@inheritDoc}
     * <br>
     * 採番対象IDに対応するシーケンスオブジェクト名を設定ファイル({@link #setIdTable(java.util.Map)}に設定された情報)
     * から取得し、採番処理を行う。
     *
     * {@link #setIdTable(java.util.Map)}に設定されたIDとシーケンスの紐付けテーブルに、
     * 指定されたIDが存在しない場合は、{@link IllegalArgumentException}を送出する。
     */
    @Override
    public String generateId(String id, IdFormatter formatter) {
        if (idTable == null || idTable.get(id) == null) {
            // 採番対象のIDに紐付くシーケンス名が取得できない場合。
            throw new IllegalArgumentException(String.format("id was not found. id:%s", id));
        }

        AppDbConnection connection = DbConnectionContext.getConnection(dbTransactionName);

        SqlPStatement statement = connection.prepareStatement(getSql(idTable.get(id)));

        SqlResultSet resultSet = statement.retrieve(1, 1);
        String generateId = resultSet.get(0).getString("GENERATE_ID");

        if (formatter != null) {
            return formatter.format(id, generateId);
        }
        return generateId;
    }

    /**
     * 採番対称を識別するIDとシーケンス名の紐付け用テーブルを設定する。<br>
     * シーケンス名を文字列連結してSQL文を生成するため、シーケンス名にスペースがあった場合はエラーとし
     * SQLインジェクションの脆弱性への対応を行う。
     *
     * @param idTable 採番対象IDテーブル
     * (key -> 採番対称を識別するID:value -> シーケンス名)
     */
    public void setIdTable(Map<String, String> idTable) {
        for (Map.Entry<String, String> entry : idTable.entrySet()) {
            String name = entry.getValue();
            if (name.indexOf(" ") != -1) {
                // ブランクがあった場合はエラーとする。
                throw new IllegalArgumentException(String.format("invalid sequence name. id = %s, sequence name = %s",
                        entry.getKey(), name));
            }
        }
        this.idTable = idTable;
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
     * SQL文を取得する。<br>
     * パラメータで指定されたシーケンス名に対応するSQLがキャッシュに存在する場合は、
     * キャッシュからSQL文を取得し、存在しない場合はSQL文を生成して返却する。
     *
     * @param sequenceName シーケンス名
     * @return シーケンス名に対応するSQL文
     */
    private String getSql(String sequenceName) {
        String sql = sqlCache.get(sequenceName);
        if (sql != null) {
            return sql;
        }

        synchronized (sqlCache) {
            sql = sqlCache.get(sequenceName);
            if (sql != null) {
                return sql;
            }
            sql = createSql(sequenceName);
            sqlCache.put(sequenceName, sql);
        }
        return sql;
    }

    /**
     * シーケンス採番用のSQL文を取得する。<br>
     * サブクラスでは、本メソッドを実装しシーケンス採番用のSQL文を生成すること。<br>
     * SELECT句に記述する採番結果の値が格納されるカラム名は、「GENERATE_ID」とすること。
     *
     * @param sequenceName 対象のシーケンス名
     * @return シーケンス取得用のSQL文
     */
    @Published(tag = "architect")
    protected abstract String createSql(String sequenceName);
}

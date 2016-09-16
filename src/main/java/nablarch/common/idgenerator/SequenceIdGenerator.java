package nablarch.common.idgenerator;

import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.db.statement.ResultSetIterator;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.log.Logger;
import nablarch.core.log.LoggerManager;
import nablarch.core.transaction.TransactionContext;

/**
 * データベースのシーケンスオブジェクトを用いて一意の値を採番するクラス。
 *
 * @author hisaaki sioiri
 */
public class SequenceIdGenerator implements IdGenerator {

    /** データベースリソース名 */
    private String dbTransactionName = TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY;

    /** ロガー */
    private static final Logger LOGGER = LoggerManager.get(SequenceIdGenerator.class);

    /**
     * シーケンスオブジェクトを用いて一意の値を採番する。
     *
     * @param sequenceName 採番対象を識別するID(シーケンスオブジェクト名)
     * @return シーケンスを元に採番した一意の値
     */
    @Override
    public String generateId(String sequenceName) {
        return generateId(sequenceName, null);
    }

    /**
     * シーケンスオブジェクトを用いて一意の値を採番する。
     *
     * @param sequenceName 採番対象を識別するID(シーケンスオブジェクト名)
     * @return シーケンスを元に採番した一意の値
     */
    @Override
    public String generateId(String sequenceName, IdFormatter formatter) {
        final TransactionManagerConnection connection =
        		DbConnectionContext.getTransactionManagerConnection(dbTransactionName);

        final SqlPStatement statement = connection.prepareStatement(
                connection.getDialect()
                        .buildSequenceGeneratorSql(sequenceName));

        final ResultSetIterator rs = statement.executeQuery();
        if (!rs.next()) {
            throw new SequenceGeneratorFailedException(sequenceName);
        }
        try {
            final String id = String.valueOf(rs.getLong(1));
            if (formatter == null) {
                return id;
            }
            return formatter.format(sequenceName, id);
        } finally {
            try {
                rs.close();
            } catch (RuntimeException e) {
                LOGGER.logWarn("failed to ResultSetIterator#close", e);
            }
        }
    }

    /**
     * トランザクション名を設定する。
     *
     * @param dbTransactionName トランザクション名
     */
    public void setDbTransactionName(String dbTransactionName) {
        this.dbTransactionName = dbTransactionName;
    }

    /**
     * シーケンス採番に失敗したことを示す例外クラス。
     */
    public static class SequenceGeneratorFailedException extends RuntimeException {

        /** 例外メッセージ */
        private static final String MESSAGE = "failed to get next value from sequence. sequence name=";

        /**
         * 例外を生成する。
         *
         * @param sequenceName シーケンス名
         */
        public SequenceGeneratorFailedException(String sequenceName) {
            super(MESSAGE + '[' + sequenceName + ']');
        }
    }
}


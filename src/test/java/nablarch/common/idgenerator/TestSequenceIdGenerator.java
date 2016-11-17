package nablarch.common.idgenerator;

/**
 * {@link SequenceIdGeneratorSupport}のテスト実施用の継承クラス。
 *
 * @author Naoki Yamamoto
 */
public class TestSequenceIdGenerator extends SequenceIdGeneratorSupport {

    @Override
    protected String createSql(String sequenceName) {
        return "SELECT * FROM SEQUENCE_TEST WHERE SEQUENCE_NAME = '" + sequenceName + "'";
    }
}

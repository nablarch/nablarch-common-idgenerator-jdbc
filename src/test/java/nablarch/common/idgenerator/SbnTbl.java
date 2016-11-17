package nablarch.common.idgenerator;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * テストテーブル
 */
@Entity
@Table(name = "SBN_TBL")
public class SbnTbl {
    
    public SbnTbl() {
    }
    
    public SbnTbl(String idCol, Long noCol) {
        this.idCol = idCol;
        this.noCol = noCol;
    }

    @Id
    @Column(name = "ID_COL", length = 2, nullable = false)
    public String idCol;
    
    @Column(name = "NO_COL", length = 5, nullable = false)
    public Long noCol;
}

package nablarch.common.idgenerator;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;

/**
 * テストテーブル
 */
@Entity
@Table(name = "SBN_TBL")
public class SbnTbl {
    
    public SbnTbl() {
    }
    
    public SbnTbl(String idCol, BigDecimal noCol) {
        this.idCol = idCol;
        this.noCol = noCol;
    }

    @Id
    @Column(name = "ID_COL", length = 2, nullable = false)
    public String idCol;
    
    @Column(name = "NO_COL", length = 5, nullable = false)
    public BigDecimal noCol;
}

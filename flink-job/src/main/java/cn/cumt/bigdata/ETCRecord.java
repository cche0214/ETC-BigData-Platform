package cn.cumt.bigdata;

import java.io.Serializable;

/**
 * ETC过车记录数据模型
 * 对应8个标准字段
 */
public class ETCRecord implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    // 过车序号（带G前缀）
    private String GCXH;
    
    // 行政区划名称
    private String XZQHMC;
    
    // 卡口名称
    private String KKMC;
    
    // 方向类型（1=进入，2=驶出）
    private String FXLX;
    
    // 过车时间（YYYY-MM-DD HH:MM:SS）
    private String GCSJ;
    
    // 号牌种类
    private String HPZL;
    
    // 号牌号码（已脱敏）
    private String HPHM;
    
    // 车辆品牌型号
    private String CLPPXH;
    
    // 无参构造函数
    public ETCRecord() {
    }
    
    // 全参构造函数
    public ETCRecord(String GCXH, String XZQHMC, String KKMC, String FXLX, 
                     String GCSJ, String HPZL, String HPHM, String CLPPXH) {
        this.GCXH = GCXH;
        this.XZQHMC = XZQHMC;
        this.KKMC = KKMC;
        this.FXLX = FXLX;
        this.GCSJ = GCSJ;
        this.HPZL = HPZL;
        this.HPHM = HPHM;
        this.CLPPXH = CLPPXH;
    }
    
    // Getters and Setters
    public String getGCXH() {
        return GCXH;
    }
    
    public void setGCXH(String GCXH) {
        this.GCXH = GCXH;
    }
    
    public String getXZQHMC() {
        return XZQHMC;
    }
    
    public void setXZQHMC(String XZQHMC) {
        this.XZQHMC = XZQHMC;
    }
    
    public String getKKMC() {
        return KKMC;
    }
    
    public void setKKMC(String KKMC) {
        this.KKMC = KKMC;
    }
    
    public String getFXLX() {
        return FXLX;
    }
    
    public void setFXLX(String FXLX) {
        this.FXLX = FXLX;
    }
    
    public String getGCSJ() {
        return GCSJ;
    }
    
    public void setGCSJ(String GCSJ) {
        this.GCSJ = GCSJ;
    }
    
    public String getHPZL() {
        return HPZL;
    }
    
    public void setHPZL(String HPZL) {
        this.HPZL = HPZL;
    }
    
    public String getHPHM() {
        return HPHM;
    }
    
    public void setHPHM(String HPHM) {
        this.HPHM = HPHM;
    }
    
    public String getCLPPXH() {
        return CLPPXH;
    }
    
    public void setCLPPXH(String CLPPXH) {
        this.CLPPXH = CLPPXH;
    }
    
    /**
     * 生成HBase的RowKey
     * 格式：时间分片(YYYYMM)_区域_车牌前2位_唯一ID
     * 示例：202312_铜山县_苏C_G320300108805918144
     * 
     * 设计原则：
     * 1. 时间分片放最前：支持按月范围查询
     * 2. 区域次之：支持按区域过滤
     * 3. 车牌前缀：支持车牌模糊查询
     * 4. 唯一ID：保证RowKey唯一性
     */
    public String generateRowKey() {
        if (GCSJ == null || GCSJ.length() < 7) {
            return "UNKNOWN_" + System.currentTimeMillis();
        }
        
        // 提取年月：2023-12-01 -> 202312
        String yearMonth = GCSJ.substring(0, 7).replace("-", "");
        
        // 提取区域（默认"未知"）
        String region = (XZQHMC != null && !XZQHMC.isEmpty()) ? XZQHMC : "未知";
        
        // 提取车牌前2位（默认"未知"）
        String platePrefix = "未知";
        if (HPHM != null && HPHM.length() >= 2) {
            platePrefix = HPHM.substring(0, 2);
        }
        
        // 唯一ID（使用GCXH）
        String uniqueId = (GCXH != null) ? GCXH : String.valueOf(System.currentTimeMillis());
        
        // 拼接RowKey
        return yearMonth + "_" + region + "_" + platePrefix + "_" + uniqueId;
    }
    
    /**
     * 数据验证：检查必需字段是否完整
     */
    public boolean isValid() {
        return GCXH != null && !GCXH.isEmpty()
            && GCSJ != null && !GCSJ.isEmpty()
            && XZQHMC != null && !XZQHMC.isEmpty()
            && KKMC != null && !KKMC.isEmpty()
            && FXLX != null && !FXLX.isEmpty();
    }
    
    @Override
    public String toString() {
        return "ETCRecord{" +
                "GCXH='" + GCXH + '\'' +
                ", XZQHMC='" + XZQHMC + '\'' +
                ", KKMC='" + KKMC + '\'' +
                ", FXLX='" + FXLX + '\'' +
                ", GCSJ='" + GCSJ + '\'' +
                ", HPZL='" + HPZL + '\'' +
                ", HPHM='" + HPHM + '\'' +
                ", CLPPXH='" + CLPPXH + '\'' +
                '}';
    }
}


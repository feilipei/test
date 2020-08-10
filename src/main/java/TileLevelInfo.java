import lombok.Data;

import java.io.Serializable;

/**
 * Created on 2020/08/10 13:46
 *
 * @author jcf
 */
@Data
public class TileLevelInfo implements Serializable {
    /**
     * 从0开始
     */
    private short level;
    /**
     * 当前级要裁剪的开始行(从0行开始，包含该行)
     */
    private int beginRow;
    /**
     * 当前级要裁剪的终止行(包含该行)
     */
    private int endRow;
    /**
     * 当前级要裁剪的开始列(从0行开始，包含该列)
     */
    private int beginCol;
    /**
     * 当前级要裁剪的终止列(包含该列)
     */
    private int endCol;
    /**
     * 使用分辨率还是比例尺
     */
    private boolean isRes = true;
    /**
     * 当前级数的比例尺分母
     */
    private double scale;
    /**
     * 当前级数的分辨率
     */
    private double resolution;

}

import com.zondy.mapgis.geodatabase.DataBase;
import com.zondy.mapgis.geodatabase.LogEventReceiver;
import com.zondy.mapgis.geodatabase.Server;
import com.zondy.mapgis.geodatabase.event.IStepStartListener;
import com.zondy.mapgis.geodatabase.raster.*;
import com.zondy.mapgis.map.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;

import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;


public class Test {
    public final static int MIN_TILE_COUNT_PER_TASK = 64 * 32;
    public final static int MAX_TILE_COUT_PER_TASK = 64 * 64;
    public final static int MAX_TILE_ROW_COUT = 64;
    public final static int MAX_TILE_COL_COUT = 64;
    /**
     * 每一级的最大任务数
     */
    private final static int MAX_TASKS_PER_LEVEL = 100;
    private static String xmlPath = "";
    private static String tileAddress = "";
    private static ExecutorService fixedThreadPoolTemp;

    //命令
    //mosaicdataset
    //-m -c [abc] -g [servername][gdbname][username][password]
    //-m -o [abc] -g [servername][gdbname][username][password]
    //-m -a [abd] -g [servername][gdbname][username][password] -p [path]
    public static void main(String[] args) {

        URLClassLoader urlClassLoader = (URLClassLoader) Test.class.getClassLoader();
        String classPaths = Arrays.stream(urlClassLoader.getURLs()).map(t -> t.toString()).collect(Collectors.joining("\n"));
        System.out.println("classpath:" + classPaths);
        System.out.println("开始");
        Options options = new Options();
        options.addOption(new Option("m", "mosaicdataset", false, "mosaicdataset operation"));
        options.addOption(new Option("c", "create", true, "create operation"));
        options.addOption(new Option("o", "open", true, "open operation"));
        options.addOption(new Option("a", "add", true, "add operation"));
        options.addOption(new Option("p", "path", true, "path"));
        options.addOption(new Option("h", "help", false, "help"));
        options.addOption(new Option("s", "imageserver", false, "help"));
        Option p = new Option("g", "gdb", true, "gdb info");

        p.setArgs(4);
        options.addOption(p);

        CommandLine cli = null;
        CommandLineParser cliParser = new DefaultParser();
        HelpFormatter helpFormatter = new HelpFormatter();

        try {
            cli = cliParser.parse(options, args);
        } catch (ParseException e) {
            // 解析失败是用 HelpFormatter 打印 帮助信息
            helpFormatter.printHelp(">>>>>> test cli options", options);
            e.printStackTrace();
        }

        if (cli.hasOption("h")) {
            System.out.println(String.format(">>>>>> create:-m -c 名称 -g 服务名 数据库名 用户名 密码"));
            System.out.println(String.format(">>>>>> open  :-m -o 名称 -g 服务名 数据库名 用户名 密码"));
            System.out.println(String.format(">>>>>> add   :-m -a 名称 -g 服务名 数据库名 用户名 密码 -p 路径"));
            System.out.println(String.format(">>>>>> imageserver   :-s -p 路径"));
            return;
        }

        //如果是影像服务
        if (cli.hasOption("s")) {
            //创建
            if (cli.hasOption("p")) {
                String path = cli.getOptionValue("p");
                Test.xmlPath = path;
                System.out.println("XML路径:" + Test.xmlPath);
                cutImage();
//                outputImage(path);
            }
        }

        //如果是镶嵌数据集操作
        if (cli.hasOption("m")) {

            //创建
            if (cli.hasOption("c")) {
                String name = cli.getOptionValue("c");
                String[] gdbInfo = cli.getOptionValues("g");
                createMosaicDataset(name, gdbInfo);

            }

            //打开
            if (cli.hasOption("o")) {
                String name = cli.getOptionValue("o");
                String[] gdbInfo = cli.getOptionValues("g");
                openMosaicDataset(name, gdbInfo);
            }

            //添加
            if (cli.hasOption("a")) {
                String name = cli.getOptionValue("a");
                String path = cli.getOptionValue("p");
                String[] gdbInfo = cli.getOptionValues("g");
                addRasterToMosaicDataset(name, gdbInfo, path);
            }
        }
    }

    public static void outputImage(String path) {
        ImageService s = new ImageService();
        s.load(path);
        Random ra = new Random();

        ImageServiceCacheInfo cacheInfo = s.getCacheInfo();
        if (cacheInfo != null) {
            ExecutorService fixedThreadPool = Executors.newFixedThreadPool(200);

            for (int i = 0; i < 100000; i++) {

                fixedThreadPool.execute(() -> {

                    int nStartLevel = cacheInfo.getStartLevel();
                    int nEndLevel = cacheInfo.getEndLevel();
                    int nLevel = 0, nRow, nCol;

                    nLevel = ra.nextInt(nEndLevel - nStartLevel + 1) + nStartLevel;

                    LongRect range = cacheInfo.getLevelTileRange(nLevel);
                    nRow = ra.nextInt((int) range.getYMax() - (int) range.getYMin() + 1) + (int) range.getYMin();
                    nCol = ra.nextInt((int) range.getXMax() - (int) range.getXMin() + 1) + (int) range.getXMin();
                    ImageResult result = s.exportImage(nLevel, nRow, nCol, ImageType.Mixed);
                    boolean f = result.getIsDisposable();
                    if (result != null) {
                        byte[] valuse = result.getData();
                        int h = result.getHeight();
                        int w = result.getWidth();
                    }

                    result.dispose();
                    //Runtime.getRuntime().gc();

                });
            }
        }
    }

    public static void createMosaicDataset(String name, String[] gdbInfo) {
        if (name == null || gdbInfo == null || gdbInfo.length < 4) {
            System.out.println(String.format(">>>>>> mosaicdataset created faile"));
            return;
        }

        Server svr = new com.zondy.mapgis.geodatabase.Server();
        if (svr.connect(gdbInfo[0], gdbInfo[2], gdbInfo[3]) <= 0) {
            System.out.println(String.format(">>>>>> failed to connect server"));
            return;
        }

        DataBase db = svr.getGdb(gdbInfo[1]);
        if (db == null) {
            svr.disConnect();
            System.out.println(String.format(">>>>>> failed to open gdb"));
            return;
        }

        MosaicDataset mds = new MosaicDataset();
        CreateProperty createProperty = new CreateProperty();
        if (!mds.create(db, name, name, 8, createProperty)) {
            svr.disConnect();
            db.close();
            System.out.println(String.format(">>>>>> failed to create mosaicdataset"));
            return;
        }

        System.out.println(String.format(">>>>>> sucess"));
        svr.disConnect();
        db.close();
    }

    public static void openMosaicDataset(String name, String[] gdbInfo) {
        if (name == null || gdbInfo == null || gdbInfo.length < 4) {
            System.out.println(String.format(">>>>>> mosaicdataset created faile"));
            return;
        }

        Server svr = new Server();
        if (svr.connect(gdbInfo[0], gdbInfo[2], gdbInfo[3]) <= 0) {
            System.out.println(String.format(">>>>>> failed to connect server"));
            return;
        }

        DataBase db = svr.getGdb(gdbInfo[1]);
        if (db == null) {
            svr.disConnect();
            System.out.println(String.format(">>>>>> failed to open gdb"));
            return;
        }

        MosaicDataset mds = new MosaicDataset();
        if (!mds.open(db, name)) {
            svr.disConnect();
            db.close();
            System.out.println(String.format(">>>>>> failed to open mosaicdataset"));
            return;
        }

        System.out.println(String.format(">>>>>> sucess"));
        svr.disConnect();
        db.close();
    }

    public static void addRasterToMosaicDataset(String name, String[] gdbInfo, String path) {
        if (name == null || gdbInfo == null || gdbInfo.length < 4) {
            System.out.println(String.format(">>>>>> mosaicdataset created faile"));
            return;
        }

        Server svr = new Server();
        if (svr.connect(gdbInfo[0], gdbInfo[2], gdbInfo[3]) <= 0) {
            System.out.println(String.format(">>>>>> failed to connect server"));
            return;
        }

        DataBase db = svr.getGdb(gdbInfo[1]);
        if (db == null) {
            svr.disConnect();
            System.out.println(String.format(">>>>>> failed to open gdb"));
            return;
        }

        MosaicDataset mds = new MosaicDataset();
        if (!mds.open(db, name)) {
            svr.disConnect();
            db.close();
            System.out.println(String.format(">>>>>> failed to open mosaicdataset"));
            return;
        }

        String[] paths = new String[1];
        RasterBuilder builder = new RasterBuilder();
        paths[0] = path;
        builder.setPaths(paths);

        AddRasterProperty props = new AddRasterProperty();
        props.setBuilder(builder);
        props.setIsUpdateBoundary(true);
        props.setIsUpdateCellsize(true);
        props.setIsUpdateOverviewState(true);
        props.setMinPixelSize(1500);

        if (!mds.addRasters(props)) {
            svr.disConnect();
            db.close();
            mds.close();
            System.out.println(String.format(">>>>>> failed to add file to mosaicdataset"));
            return;
        }

        System.out.println(String.format(">>>>>> sucess"));
        svr.disConnect();
        db.close();
    }

    public static void cutImage() {
        ImageService s = new ImageService();
        boolean load = s.load(Test.xmlPath);
        if (load == false) {
            System.out.println("XML加载失败");
        }
        ImageServiceCacheInfo cacheInfo = s.getCacheInfo();
        int startLevel = cacheInfo.getStartLevel();
        int endLevel = cacheInfo.getEndLevel();
        String cachePath = cacheInfo.getCachePath();
        Test.tileAddress = cachePath;
        System.out.println("MongoDB路径：" + Test.tileAddress);
        List<TileLevelInfo> infos = new ArrayList<>();
        for (int i = startLevel; i <= endLevel; i++) {
            TileLevelInfo tileLevelInfo = new TileLevelInfo();
            short level = (short) (i - 1);
            tileLevelInfo.setLevel(level);
            tileLevelInfo.setBeginCol((int) cacheInfo.getLevelTileRange(i).getXMin());
            tileLevelInfo.setEndCol((int) cacheInfo.getLevelTileRange(i).getXMax());
            tileLevelInfo.setBeginRow((int) cacheInfo.getLevelTileRange(i).getYMin());
            tileLevelInfo.setEndRow((int) cacheInfo.getLevelTileRange(i).getYMax());
            infos.add(tileLevelInfo);
        }
        createSubTaskSNew(infos, startLevel - 1, endLevel - 1);

    }

    public static void createSubTaskSNew(List<TileLevelInfo> infos, int preStartLevel, int preEndLevel) {
        Test.fixedThreadPoolTemp = Executors.newFixedThreadPool(32);
        for (int i = preStartLevel; i <= preEndLevel; i++) {
            TileLevelInfo levelInfo = getLevelInfo(infos, i);
            if (levelInfo != null) {
                short level = levelInfo.getLevel();
                int beginRow = levelInfo.getBeginRow();
                int endRow = levelInfo.getEndRow();
                int beginCol = levelInfo.getBeginCol();
                int endCol = levelInfo.getEndCol();
                //计算当前缓存级数行数
                int totalRow = endRow - beginRow + 1;
                //计算当前缓存级数列数
                int totalCol = endCol - beginCol + 1;
                //计算当前缓存级数瓦片总数
                int currentLevelTotal = totalRow * totalCol;

                //瓦片总数不大于MAX_TILE_COUT_PER_TASK时按一个任务划分
                if (currentLevelTotal <= MAX_TILE_COUT_PER_TASK) {
                    createSubTaskEntity(level, level, beginRow, endRow, beginCol, endCol, currentLevelTotal, preStartLevel, preEndLevel);
                    continue;
                }

                int totalTasks = (int) Math.floor((currentLevelTotal / MAX_TILE_COUT_PER_TASK));
                //切割块X方向瓦片数
                int xSize = MAX_TILE_ROW_COUT;
                //切割块Y方向瓦片数
                int ySize = MAX_TILE_COL_COUT;
                //切割块总瓦片数
                int maxTileBlockSize = MAX_TILE_COUT_PER_TASK;

                //如果当前级别任务数超过100，则缩减到100左右
                if (totalTasks > MAX_TASKS_PER_LEVEL) {
                    //重新计算每个切割块大小
                    maxTileBlockSize = (int) Math.ceil(currentLevelTotal / MAX_TILE_COUT_PER_TASK);
                    int size = (int) Math.ceil(Math.sqrt(maxTileBlockSize));
                    if (size % 2 != 0) {
                        size += 1;
                    }
                    maxTileBlockSize = size * size;
                    xSize = size;
                    ySize = size;
                } else {
                    xSize = MAX_TILE_ROW_COUT;
                    ySize = MAX_TILE_COL_COUT;
                    maxTileBlockSize = MAX_TILE_ROW_COUT * MAX_TILE_COL_COUT;
                }

                //切割块形状根据数据的行列号变化
                if (totalCol < xSize) {
                    while (totalCol <= xSize) {
                        xSize = xSize / 2;
                    }
                    ySize = maxTileBlockSize / xSize;
                } else {
                    if (totalRow < ySize) {
                        while (totalRow <= ySize) {
                            ySize = ySize / 2;
                        }
                        xSize = maxTileBlockSize / ySize;
                    }
                }
                createSub(xSize, ySize, level, beginRow, endRow, beginCol, endCol, preStartLevel, preEndLevel);
            }
        }
    }

    private static TileLevelInfo getLevelInfo(List<TileLevelInfo> list, int level) {
        for (TileLevelInfo levelInfo : list) {
            if (levelInfo.getLevel() == level) {
                return levelInfo;
            }
        }
        return null;
    }

    private static void createSub(int xSize, int ySize, short level, int beginRow, int endRow, int beginCol, int endCol, int preStartLevel, int preEndLevel) {
        //计算当前缓存级数列数
        int totalCol = endCol - beginCol + 1;
        //计算当前缓存级数行数
        int totalRow = endRow - beginRow + 1;
        int startC = beginCol;
        int endC = endCol;
        int startR = beginRow;
        int endR = endRow;

        //X轴方向可切割次数
        int xTask = totalCol / xSize;
        int xTemp = totalCol % xSize;
        if (xTemp != 0) {
            xTask += 1;
        }

        //Y轴方向可切割次数
        int yTask = totalRow / ySize;
        int yTemp = totalRow % ySize;
        if (yTemp != 0) {
            yTask += 1;
        }

        //外层循环遍历Y轴方向切割次数，计算子任务的起止行号
        for (int n = 0; n < yTask; n++) {
            startR = beginRow + n * ySize;
            if (n == (yTask - 1)) {
                endR = endRow;
            } else {
                endR = beginRow + (n + 1) * ySize - 1;
            }
            //外层循环遍历X轴方向切割次数，计算子任务的起止列号
            for (int m = 0; m < xTask; m++) {
                startC = beginCol + m * xSize;
                if (m == (xTask - 1)) {
                    endC = endCol;
                } else {
                    endC = beginCol + (m + 1) * xSize - 1;
                }
                //创建子任务实体
                long total = (endR - startR + 1) * (endC - startC + 1);
                createSubTaskEntity(level, level, startR, endR, startC, endC, total, preStartLevel, preEndLevel);
            }
        }

    }

    private static void createSubTaskEntity(int beginLevel, int endLevel, int beginRow, int endRow, int beginCol, int endCol, long total, int preStartLevel, int preEndLevel) {
        //执行影像裁剪任务
        Test.fixedThreadPoolTemp.submit(() -> {
            try {
                executeCut(beginLevel + 1, beginRow, beginCol, endRow - beginRow + 1, endCol - beginCol + 1, CacheUpdateMode.RecreateAll);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    public static boolean executeCut(int level, int beginRow, int beginCol, int rows, int cols, CacheUpdateMode mode) throws Exception {
        ImageService imageServer = new ImageService();
        imageServer.load(Test.xmlPath);
        String url = Test.tileAddress;
        SourceType sourceType = imageServer.getServiceInfo().getSourceType();
        RasterCacheCooker imageCacheCooker = new RasterCacheCooker(sourceType);
        switch (sourceType) {
            case Source_None:
                throw new Exception("数据源类型错误" + sourceType.toString());
            case Source_Dataset:
                imageCacheCooker.setDataset(imageServer.getDataset());
                break;
            case Source_Catalog:
                imageCacheCooker.setCatalog(imageServer.getCatalog());
                break;
            case Source_MosaicDataset:
                imageCacheCooker.setMosaicDataset(imageServer.getMosaicDataset());
                break;
            case Source_MapsetDataset:
                throw new Exception("未实现的类型" + sourceType.toString());
            default:
                imageCacheCooker = null;
                break;
        }
        if (imageCacheCooker == null) {
            throw new Exception("影像数据类型不匹配:" + sourceType.toString());
        }
        imageCacheCooker.setRasterResampling(RasterResampling.NearestNeighbor);
        imageCacheCooker.setGroupSize(1024);
        imageCacheCooker.setPath(url);
        if (imageCacheCooker.connect()) {
            System.out.println(String.format("执行缓存生成任务开始:level:%s, beginRow:%s, beginCol:%s, rows:%s, cols:%s, mode:%s", level, beginRow, beginCol, rows, cols, mode.toString()));
            long startTime = System.currentTimeMillis();
            boolean result = false;
            LogEventReceiver logEvent = new LogEventReceiver();
            try {
                logEvent.addStepStartListener(new IStepStartListener() {
                    @Override
                    public void onStepStart(String s) {
                        System.out.println(s);
                    }
                });
                result = imageCacheCooker.updateCache(level, beginRow, beginCol, rows, cols, mode, logEvent);
                if (!result) {
                    System.out.println(String.format("执行缓存生成任务调用底层接口返回false:level:%s, beginRow:%s, beginCol:%s, rows:%s, cols:%s, mode:%s, info:%s", level, beginRow, beginCol, rows, cols, mode.toString(), "imageCacheCooker.updateCache执行返回false"));
                }
            } catch (Exception ex) {
                System.out.println(String.format("执行缓存生成任务调用底层接口出现异常:level:%s, beginRow:%s, beginCol:%s, rows:%s, cols:%s, mode:%s, info:%s", level, beginRow, beginCol, rows, cols, mode.toString(), ex.toString()));
                result = false;
            }
            long endTime = System.currentTimeMillis();
            System.out.println(String.format("执行缓存生成任务完成:level:%s, beginRow:%s, beginCol:%s, rows:%s, cols:%s, mode:%s, result:%s, time:%ss", level, beginRow, beginCol, rows, cols, mode.toString(), result, (endTime - startTime) / 60));
            return result;
        }
        System.out.println("MongoDB数据库连接失败:" + url);
        throw new Exception("数据库连接失败:" + url);
    }
}

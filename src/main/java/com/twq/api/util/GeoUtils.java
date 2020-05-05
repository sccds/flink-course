package com.twq.api.util;

public class GeoUtils {
    // 纽约地理位置范围
    // 最东边的经度
    public static double LON_EAST = -73.7;
    // 最西边的经度
    public static double LON_WEST = -74.05;
    // 最北边的纬度
    public static double LAT_NORTH = 41.0;
    // 最南边的纬度
    public static double LAT_SOUTH = 40.5;

    // 将纽约市划分成 250 * 400 个网格
    // DELTA_LON = (|LON_WEST| - |LON_EAST|) / 250
    public static double DELTA_LON = 0.0014;
    // DELTA_LAT = (|LAT_NORTH| - |LAT_SOUTH|) / 400
    public static double DELTA_LAT = 0.00125;

    // 在经度上划分成 250 份
    public static int NUMBER_OF_GRID_X = 250;
    // 在纬度上划分成 400 份
    public static int NUMBER_OF_GRID_Y = 400;

    // 判断指定经纬度是否在纽约地理范围内
    public static boolean isInNYC(float lon, float lat) {
        return !(lon > LON_EAST || lon < LON_WEST) &&
                    !(lat > LAT_NORTH || lat < LAT_SOUTH);
    }

    /**
     *  将纽约地理范围划分成 250 * 400 个单元格
     *  这个方法的作用是计算指定的经纬度所在的单元格 id
     *
     * @param lon 指定的经度
     * @param lat 指定的纬度
     *
     * @return 指定经纬度所在的单元格 id.
     */
    public static int mapToGridCell(float lon, float lat) {
        // 计算 x 坐标方向所在的单元格
        int xIndex = (int)Math.floor((Math.abs(LON_WEST) - Math.abs(lon)) / DELTA_LON);
        // 计算 y 坐标方向所在的单元格
        int yIndex = (int)Math.floor((LAT_NORTH - lat) / DELTA_LAT);
        // 返回所在单元格的 id = x + (y * 250) ( x 方向被分割成了 250 个单元格)
        return xIndex + (yIndex * NUMBER_OF_GRID_X);
    }
}

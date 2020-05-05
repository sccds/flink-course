package com.twq.api.datatypes;

import com.twq.api.util.GeoUtils;

public class EnrichedTaxiRide extends TaxiRide {
    // 起始位置所属网格 ID
    private int startCell;

    // 终止位置
    private int endCell;

    public EnrichedTaxiRide() {}

    public EnrichedTaxiRide(TaxiRide taxiRide) {
        super.copyData(taxiRide);
        this.startCell = GeoUtils.mapToGridCell(taxiRide.getStartLon(), taxiRide.getStartLat());
        this.endCell = GeoUtils.mapToGridCell(taxiRide.getEndLon(), taxiRide.getEndLat());
    }

    public int getStartCell() {
        return startCell;
    }

    public int getEndCell() {
        return endCell;
    }

    @Override
    public String toString() {
        return "EnrichedTaxiRide{" +
                "startCell=" + startCell +
                ", endCell=" + endCell + ", " +
                super.toString() +
                '}';
    }
}

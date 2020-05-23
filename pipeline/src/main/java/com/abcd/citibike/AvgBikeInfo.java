package com.abcd.citibike;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.util.List;

@DefaultCoder(AvroCoder.class)
public class AvgBikeInfo {
    private String stationID;
    private double avgAvailableBikes;
    private double avgDisabledBikes;
    private double avgAvailableDocks;
    private double avgDisabledDocks;
    private String timestamp;

    public AvgBikeInfo() {
    }

    public static AvgBikeInfo newAvgBikeInfo(String line) {
        String[] info = line.split(",");
        AvgBikeInfo newInfo = new AvgBikeInfo();
        newInfo.stationID = info[0];
        newInfo.avgAvailableBikes = Double.parseDouble(info[1]);
        newInfo.avgDisabledBikes = Double.parseDouble(info[2]);
        newInfo.avgAvailableDocks = Double.parseDouble(info[3]);
        newInfo.avgDisabledDocks = Double.parseDouble(info[4]);
        newInfo.timestamp = info[5];
        return newInfo;
    }

    public String getStationID() {
        return stationID;
    }

    public double getAvgAvailableBikes() {
        return avgAvailableBikes;
    }

    public double getAvgDisabledBikes() {
        return avgDisabledBikes;
    }

    public double getAvgAvailableDocks() {
        return avgAvailableDocks;
    }

    public double getAvgDisabledDocks() {
        return avgDisabledDocks;
    }

    public String getTimestamp() {
        return timestamp;
    }
}


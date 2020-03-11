package com.abcd.citibike;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

 @DefaultCoder(AvroCoder.class)
public class BikeInfo {
	private String stationID;
	private int availableBikes;
	private int disabledBikes;
	private int availableDocks;
	private int disabledDocks;
	private boolean availableKeyID;
	private boolean rentStatus;

	public BikeInfo() {
	}

	public static BikeInfo newBikeInfo(String line) {
		JSONParser parser = new JSONParser();
		JSONObject jsonObject = null;
		try {
			jsonObject = (JSONObject) parser.parse(line);
		} catch (ParseException err) {
			err.printStackTrace();
		}
		BikeInfo newInfo = new BikeInfo();
		newInfo.stationID = jsonObject.get("station_id").toString();
		newInfo.availableBikes = ((Long) jsonObject.get("available_bikes")).intValue();
		newInfo.disabledBikes = ((Long) jsonObject.get("disabled_bikes")).intValue();
		newInfo.availableDocks = ((Long) jsonObject.get("available_docks")).intValue();
		newInfo.disabledDocks = ((Long) jsonObject.get("disabled_docks")).intValue();
		newInfo.rentStatus = (boolean) jsonObject.get("rent_status");
		return newInfo;
	}

	public void setStationID(String stationID) {
		this.stationID = stationID;
	}

	public void setAvailableBikes(int availableBikes) {
		this.availableBikes = availableBikes;
	}

	public void setDisabledBikes(int disabledBikes) {
		this.disabledBikes = disabledBikes;
	}

	public void setAvailableDocks(int availableDocks) {
		this.availableDocks = availableDocks;
	}

	public void setDisabledDocks(int disabledDocks) {
		this.disabledDocks = disabledDocks;
	}

	public void setAvailableKeyID(boolean availableKeyID) {
		this.availableKeyID = availableKeyID;
	}

	public void setRentStatus(boolean rentStatus) {
		this.rentStatus = rentStatus;
	}

	public static void main(String[] args) {
		String test = "{\"station_id\": 304, \"available_key_id\": true, \"available_bikes\": 21, \"disabled_bikes\": 2, \"available_docks\": 10, \"disabled_docks\": 0, \"rent_status\": true, \"last_reported\": 1583431113}";
//		newBikeInfo(test);
	}

	public String getStationID() {
		return stationID;
	}

	public int getAvailableBikes() {
		return availableBikes;
	}

	public int getDisabledBikes() {
		return disabledBikes;
	}

	public int getAvailableDocks() {
		return availableDocks;
	}
	public int getDisabledDocks() {
		return disabledDocks;
	}

	public boolean isAvailableKeyID() {
		return availableKeyID;
	}

	public boolean isRentStatus() {
		return rentStatus;
	}
}


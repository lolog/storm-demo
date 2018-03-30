package dj.storm.domain;

import java.io.Serializable;

public class DiagnosisEvent implements Serializable {
	private static final long serialVersionUID = -4282865493053403235L;
	private double lng;
	private double lat;
	private long time;
	private long code;
	public DiagnosisEvent() {
		
	}
	public DiagnosisEvent(double lng, double lat, long time, long code) {
		this.lng = lng;
		this.lat = lat;
		this.time = time;
		this.code = code;
	}
	public double getLng() {
		return lng;
	}
	public void setLng(double lng) {
		this.lng = lng;
	}
	public double getLat() {
		return lat;
	}
	public void setLat(double lat) {
		this.lat = lat;
	}
	public long getTime() {
		return time;
	}
	public void setTime(long time) {
		this.time = time;
	}
	public long getCode() {
		return code;
	}
	public void setCode(long code) {
		this.code = code;
	}
	@Override
	public String toString() {
		return "DiagnosisEvent [lng=" + lng + ", lat=" + lat + ", time=" + time
				+ ", code=" + code + "]";
	}
}

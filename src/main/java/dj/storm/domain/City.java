package dj.storm.domain;

import java.io.Serializable;

public class City implements Serializable {
	private static final long serialVersionUID = 1309246832914650074L;
	private String cityName;
	private double minLng;
	private double maxLng;
	private double minLat;
	private double maxLat;
	public City() {
	}
	public City(String cityName,double minLng,double maxLng,double minLat,double maxLat) {
		this.cityName = cityName;
		this.minLng = minLng;
		this.maxLng = maxLng;
		this.minLat = minLat;
		this.maxLat = maxLat;
	}
	public String getCityName() {
		return cityName;
	}
	public void setCityName(String cityName) {
		this.cityName = cityName;
	}
	public double getMinLng() {
		return minLng;
	}
	public void setMinLng(double minLng) {
		this.minLng = minLng;
	}
	public double getMaxLng() {
		return maxLng;
	}
	public void setMaxLng(double maxLng) {
		this.maxLng = maxLng;
	}
	public double getMinLat() {
		return minLat;
	}
	public void setMinLat(double minLat) {
		this.minLat = minLat;
	}
	public double getMaxLat() {
		return maxLat;
	}
	public void setMaxLat(double maxLat) {
		this.maxLat = maxLat;
	}
	@Override
	public String toString() {
		return "City [cityName=" + cityName + ", minLng=" + minLng
				+ ", maxLng=" + maxLng + ", minLat=" + minLat + ", maxLat="
				+ maxLat + "]";
	}
}

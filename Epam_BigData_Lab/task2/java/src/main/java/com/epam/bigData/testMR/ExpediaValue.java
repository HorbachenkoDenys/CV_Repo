package com.epam.bigData.testMR;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ExpediaValue implements Writable {
    private String dateTime;
    private Integer siteName;
    private Integer posaContinent;
    private Integer userLocationCountry;
    private Integer userLocationRegion;
    private Integer userLocationCity;
    private Double origDestinationDistance;
    private Integer userId;
    private Integer isMobile;
    private Integer isPackage;
    private Integer channel;
    private String srchCo;
    private Integer srchAdultsCount;
    private Integer srchChildrenCount;
    private Integer srchRmCount;
    private Integer srchDestinationId;
    private Integer srchDectinationTypeId;

    public ExpediaValue(){

    }

    public ExpediaValue(String dateTime, Integer siteName, Integer posaContinent, Integer userLocationCountry, Integer userLocationRegion, Integer userLocationCity, Double origDestinationDistance, Integer userId, Integer isMobile, Integer isPackage, Integer channel, String srchCo, Integer srchAdultsCount, Integer srchChildrenCount, Integer srchRmCount, Integer srchDestinationId, Integer srchDectinationTypeId) {
        this.dateTime = dateTime;
        this.siteName = siteName;
        this.posaContinent = posaContinent;
        this.userLocationCountry = userLocationCountry;
        this.userLocationRegion = userLocationRegion;
        this.userLocationCity = userLocationCity;
        this.origDestinationDistance = origDestinationDistance;
        this.userId = userId;
        this.isMobile = isMobile;
        this.isPackage = isPackage;
        this.channel = channel;
        this.srchCo = srchCo;
        this.srchAdultsCount = srchAdultsCount;
        this.srchChildrenCount = srchChildrenCount;
        this.srchRmCount = srchRmCount;
        this.srchDestinationId = srchDestinationId;
        this.srchDectinationTypeId = srchDectinationTypeId;
    }

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }

    public Integer getSiteName() {
        return siteName;
    }

    public void setSiteName(String siteName) {
        this.siteName = Integer.parseInt(siteName);
    }

    public Integer getPosaContinent() {
        return posaContinent;
    }

    public void setPosaContinent(String posaContinent) {
        this.posaContinent = Integer.parseInt(posaContinent);
    }

    public Integer getUserLocationCountry() {
        return userLocationCountry;
    }

    public void setUserLocationCountry(String userLocationCountry) {
        this.userLocationCountry = Integer.parseInt(userLocationCountry);
    }

    public Integer getUserLocationRegion() {
        return userLocationRegion;
    }

    public void setUserLocationRegion(String userLocationRegion) {
        this.userLocationRegion = Integer.parseInt(userLocationRegion);
    }

    public Integer getUserLocationCity() {
        return userLocationCity;
    }

    public void setUserLocationCity(String userLocationCity) {
        this.userLocationCity = Integer.parseInt(userLocationCity);
    }

    public Double getOrigDestinationDistance() {
        return origDestinationDistance;
    }

    public void setOrigDestinationDistance(String origDestinationDistance) {
        this.origDestinationDistance = Double.parseDouble(origDestinationDistance);
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = Integer.parseInt(userId);
    }

    public Integer getIsMobile() {
        return isMobile;
    }

    public void setIsMobile(String isMobile) {
        this.isMobile = Integer.parseInt(isMobile);
    }

    public Integer getIsPackage() {
        return isPackage;
    }

    public void setIsPackage(String isPackage) {
        this.isPackage = Integer.parseInt(isPackage);
    }

    public Integer getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = Integer.parseInt(channel);
    }

    public String getSrchCo() {
        return srchCo;
    }

    public void setSrchCo(String srchCo) {
        this.srchCo = srchCo;
    }

    public Integer getSrchAdultsCount() {
        return srchAdultsCount;
    }

    public void setSrchAdultsCount(String srchAdultsCount) {
        this.srchAdultsCount = Integer.parseInt(srchAdultsCount);
    }

    public Integer getSrchChildrenCount() {
        return srchChildrenCount;
    }

    public void setSrchChildrenCount(String srchChildrenCount) {
        this.srchChildrenCount = Integer.parseInt(srchChildrenCount);
    }

    public Integer getSrchRmCount() {
        return srchRmCount;
    }

    public void setSrchRmCount(String srchRmCount) {
        this.srchRmCount = Integer.parseInt(srchRmCount);
    }

    public Integer getSrchDestinationId() {
        return srchDestinationId;
    }

    public void setSrchDestinationId(String srchDestinationId) {
        this.srchDestinationId = Integer.parseInt(srchDestinationId);
    }

    public Integer getSrchDectinationTypeId() {
        return srchDectinationTypeId;
    }

    public void setSrchDectinationTypeId(String srchDectinationTypeId) {
        this.srchDectinationTypeId = Integer.parseInt(srchDectinationTypeId);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeChars(dateTime);
        dataOutput.writeInt(siteName);
        dataOutput.writeInt(posaContinent);
        dataOutput.writeInt(userLocationCountry);
        dataOutput.writeInt(userLocationRegion);
        dataOutput.writeInt(userLocationCity);
        dataOutput.writeDouble(origDestinationDistance);
        dataOutput.writeInt(userId);
        dataOutput.writeInt(isMobile);
        dataOutput.writeInt(isPackage);
        dataOutput.writeInt(channel);
        dataOutput.writeChars(srchCo);
        dataOutput.writeInt(srchAdultsCount);
        dataOutput.writeInt(srchChildrenCount);
        dataOutput.writeInt(srchRmCount);
        dataOutput.writeInt(srchDestinationId);
        dataOutput.writeInt(srchDectinationTypeId);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        dateTime = dataInput.readLine();
        siteName = dataInput.readInt();
        posaContinent = dataInput.readInt();
        userLocationCountry = dataInput.readInt();
        userLocationRegion = dataInput.readInt();
        userLocationCity = dataInput.readInt();
        origDestinationDistance = dataInput.readDouble();
        userId = dataInput.readInt();
        isMobile = dataInput.readInt();
        isPackage = dataInput.readInt();
        channel = dataInput.readInt();
        srchCo = dataInput.readLine();
        srchAdultsCount = dataInput.readInt();
        srchChildrenCount = dataInput.readInt();
        srchRmCount = dataInput.readInt();
        srchDestinationId = dataInput.readInt();
        srchDectinationTypeId = dataInput.readInt();
    }

    @Override
    public String toString() {
        return "ExpediaValue{" +
                "dateTime=" + dateTime +
                ", siteName=" + siteName +
                ", posaContinent=" + posaContinent +
                ", userLocationCountry=" + userLocationCountry +
                ", userLocationRegion=" + userLocationRegion +
                ", userLocationCity=" + userLocationCity +
                ", origDestinationDistance=" + origDestinationDistance +
                ", userId=" + userId +
                ", isMobile=" + isMobile +
                ", isPackage=" + isPackage +
                ", channel=" + channel +
                ", srchCo=" + srchCo +
                ", srchAdultsCount=" + srchAdultsCount +
                ", srchChildrenCount=" + srchChildrenCount +
                ", srchRmCount=" + srchRmCount +
                ", srchDestinationId=" + srchDestinationId +
                ", srchDestinationTypeId=" + srchDectinationTypeId +
                '}';
    }
}

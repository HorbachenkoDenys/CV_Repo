package com.horbachenkodenis;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Objects;

/**
 * The CompositeKey is a combination of the natural (the 'hotelId' field) key and the secondary key(the 'srchCI' and 'id' fields).
 */
public class CompositeKey implements Writable, WritableComparable<CompositeKey> {
    private Long id;
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
    private String srchCI;
    private String srchCO;
    private Integer srchAdultsCount;
    private Integer srchChildrenCount;
    private Integer srchRmCount;
    private Integer srchDestinationId;
    private Integer srchDestinationTypeId;
    private Long hotelId;

    public CompositeKey() {
    }

    public CompositeKey(Long hotelId, String srchCI, Long id) {
        this.hotelId = hotelId;
        this.srchCI = srchCI;
        this.id = id;
    }

    public CompositeKey(Long id, String dateTime, Integer siteName,
                        Integer posaContinent, Integer userLocationCountry,
                        Integer userLocationRegion, Integer userLocationCity,
                        Double origDestinationDistance, Integer userId,
                        Integer isMobile, Integer isPackage, Integer channel,
                        String srchCI, String srchCO, Integer srchAdultsCount,
                        Integer srchChildrenCount, Integer srchRmCount,
                        Integer srchDestinationId, Integer srchDestinationTypeId,
                        Long hotelId) {
        this.id = id;
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
        this.srchCI = srchCI;
        this.srchCO = srchCO;
        this.srchAdultsCount = srchAdultsCount;
        this.srchChildrenCount = srchChildrenCount;
        this.srchRmCount = srchRmCount;
        this.srchDestinationId = srchDestinationId;
        this.srchDestinationTypeId = srchDestinationTypeId;
        this.hotelId = hotelId;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(id);
        dataOutput.writeUTF(dateTime);
        dataOutput.writeInt(siteName);
        dataOutput.writeInt(posaContinent);
        dataOutput.writeInt(userLocationCountry);
        dataOutput.writeInt(userLocationRegion);
        dataOutput.writeInt(userLocationCity);
        if (origDestinationDistance != null) {
            dataOutput.writeBoolean(true);
            dataOutput.writeDouble(origDestinationDistance);
        } else {
            dataOutput.writeBoolean(false);
        }
        dataOutput.writeInt(userId);
        dataOutput.writeInt(isMobile);
        dataOutput.writeInt(isPackage);
        dataOutput.writeInt(channel);
        dataOutput.writeUTF(srchCI);
        dataOutput.writeUTF(srchCO);
        dataOutput.writeInt(srchAdultsCount);
        dataOutput.writeInt(srchChildrenCount);
        dataOutput.writeInt(srchRmCount);
        dataOutput.writeInt(srchDestinationId);
        dataOutput.writeInt(srchDestinationTypeId);
        dataOutput.writeLong(hotelId);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readLong();
        dateTime = dataInput.readUTF();
        siteName = dataInput.readInt();
        posaContinent = dataInput.readInt();
        userLocationCountry = dataInput.readInt();
        userLocationRegion = dataInput.readInt();
        userLocationCity = dataInput.readInt();
        if (dataInput.readBoolean()) {
            origDestinationDistance = dataInput.readDouble();
        } else {
            origDestinationDistance = null;
        }
        userId = dataInput.readInt();
        isMobile = dataInput.readInt();
        isPackage = dataInput.readInt();
        channel = dataInput.readInt();
        srchCI = dataInput.readUTF();
        srchCO = dataInput.readUTF();
        srchAdultsCount = dataInput.readInt();
        srchChildrenCount = dataInput.readInt();
        srchRmCount = dataInput.readInt();
        srchDestinationId = dataInput.readInt();
        srchDestinationTypeId = dataInput.readInt();
        hotelId = dataInput.readLong();
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

    public void setSiteName(Integer siteName) {
        this.siteName = siteName;
    }

    public Integer getPosaContinent() {
        return posaContinent;
    }

    public void setPosaContinent(Integer posaContinent) {
        this.posaContinent = posaContinent;
    }

    public Integer getUserLocationCountry() {
        return userLocationCountry;
    }

    public void setUserLocationCountry(Integer userLocationCountry) {
        this.userLocationCountry = userLocationCountry;
    }

    public Integer getUserLocationRegion() {
        return userLocationRegion;
    }

    public void setUserLocationRegion(Integer userLocationRegion) {
        this.userLocationRegion = userLocationRegion;
    }

    public Integer getUserLocationCity() {
        return userLocationCity;
    }

    public void setUserLocationCity(Integer userLocationCity) {
        this.userLocationCity = userLocationCity;
    }

    public Double getOrigDestinationDistance() {
        return origDestinationDistance;
    }

    public void setOrigDestinationDistance(Double origDestinationDistance) {
        this.origDestinationDistance = origDestinationDistance;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public Integer getMobile() {
        return isMobile;
    }

    public void setMobile(Integer mobile) {
        isMobile = mobile;
    }

    public Integer getPackage() {
        return isPackage;
    }

    public void setPackage(Integer aPackage) {
        isPackage = aPackage;
    }

    public Integer getChannel() {
        return channel;
    }

    public void setChannel(Integer channel) {
        this.channel = channel;
    }

    public String getSrchCO() {
        return srchCO;
    }

    public void setSrchCO(String srchCO) {
        this.srchCO = srchCO;
    }

    public Integer getSrchAdultsCount() {
        return srchAdultsCount;
    }

    public void setSrchAdultsCount(Integer srchAdultsCount) {
        this.srchAdultsCount = srchAdultsCount;
    }

    public Integer getSrchChildrenCount() {
        return srchChildrenCount;
    }

    public void setSrchChildrenCount(Integer srchChildrenCount) {
        this.srchChildrenCount = srchChildrenCount;
    }

    public Integer getSrchRmCount() {
        return srchRmCount;
    }

    public void setSrchRmCount(Integer srchRmCount) {
        this.srchRmCount = srchRmCount;
    }

    public Integer getSrchDestinationId() {
        return srchDestinationId;
    }

    public void setSrchDestinationId(Integer srchDestinationId) {
        this.srchDestinationId = srchDestinationId;
    }

    public Integer getSrchDestinationTypeId() {
        return srchDestinationTypeId;
    }

    public void setSrchDestinationTypeId(Integer srchDestinationTypeId) {
        this.srchDestinationTypeId = srchDestinationTypeId;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getSrchCI() {
        return srchCI;
    }

    public void setSrchCI(String srchCI) {
        this.srchCI = srchCI;
    }

    public Long getHotelId() {
        return hotelId;
    }

    public void setHotelId(Long hotelId) {
        this.hotelId = hotelId;
    }

    public int compareTo(CompositeKey o) {
        int cmp = hotelId.compareTo(o.getHotelId());
        if (cmp != 0) {
            return cmp;
        }
        cmp = srchCI.toLowerCase().compareTo(o.getSrchCI().toLowerCase());
        if (cmp != 0) {
            return cmp;
        }
        cmp = id.compareTo(o.getId());
        return cmp;
    }

    @Override
    public String toString() {
        return "CompositeKey{" +
                "id=" + id +
                ", dateTime='" + dateTime + '\'' +
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
                ", srchCI='" + srchCI + '\'' +
                ", srchCO='" + srchCO + '\'' +
                ", srchAdultsCount=" + srchAdultsCount +
                ", srchChildrenCount=" + srchChildrenCount +
                ", srchRmCount=" + srchRmCount +
                ", srchDestinationId=" + srchDestinationId +
                ", srchDestinationTypeId=" + srchDestinationTypeId +
                ", hotelId=" + hotelId +
                '}';
    }
}



package com.yesbank.camptriggers;

import java.util.HashMap;
import java.util.Map;

public class Event {
	private String eventname;

	private String channelId;
	private String channelName;
	private String destinationAttribute;
	private boolean isValid;
	private Map<String,String>customAttr;
	private String uniqueUserid;
	

	public String getUniqueUserid() {
		return uniqueUserid;
	}
	public void setUniqueUserid(String uniqueUserid) {
		this.uniqueUserid = uniqueUserid;
	}
	public Map<String, String> getCustomAttr() {
		return customAttr;
	}
	public void setCustomAttr(Map<String, String> customAttr) {
		this.customAttr = customAttr;
	}
	public Event(String eventname, String channelId, String channelName, String destinationAttribute) {
		super();
		this.eventname = eventname;
		this.channelId = channelId;
		this.channelName = channelName;
		this.destinationAttribute = destinationAttribute;
		this.isValid=false;
		this.customAttr=new HashMap<String,String>();
	}
	public boolean isValid() {
		return isValid;
	}
	public void setValid(boolean isValid) {
		this.isValid = isValid;
	}
	public Event() {
		// TODO Auto-generated constructor stub
	}
	public String getEventname() {
		return eventname;
	}
	public void setEventname(String eventname) {
		this.eventname = eventname;
	}
	public String getChannelId() {
		return channelId;
	}
	public void setChannelId(String channelId) {
		this.channelId = channelId;
	}
	public String getChannelName() {
		return channelName;
	}
	public void setChannelName(String channelName) {
		this.channelName = channelName;
	}
	public String getDestinationAttribute() {
		return destinationAttribute;
	}
	public void setDestinationAttribute(String destinationAttribute) {
		this.destinationAttribute = destinationAttribute;
	}
	@Override
	public String toString() {
		return "Event [eventname=" + eventname + ", channelId=" + channelId + ", channelName=" + channelName
				+ ", destinationAttribute=" + destinationAttribute + ", isValid=" + isValid + ", customAttr="
				+ customAttr + ", uniqueUserid=" + uniqueUserid + "]";
	}


	
	






}

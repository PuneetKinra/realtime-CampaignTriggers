package com.yesbank.camptriggers;

public class Response {
	private Long responseCode;
	private String responseMsg;
	public Response(Long responseCode, String responseMsg) {
		super();
		this.responseCode = responseCode;
		this.responseMsg = responseMsg;
	}
	public Long getResponseCode() {
		return responseCode;
	}
	public void setResponseCode(Long responseCode) {
		this.responseCode = responseCode;
	}
	public String getResponseMsg() {
		return responseMsg;
	}
	public void setResponseMsg(String responseMsg) {
		this.responseMsg = responseMsg;
	}
	@Override
	public String toString() {
		return "Response [responseCode=" + responseCode + ", responseMsg=" + responseMsg + "]";
	}
	
	

}

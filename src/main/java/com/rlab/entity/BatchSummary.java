package com.rlab.entity;

import java.io.Serializable;
import java.util.Map;

public class BatchSummary implements Serializable {
	private String batchId;
	private int batchSize;  //count 
	private int n0;  //num no churn predictions 
	private int noOffers;  
	private int giftOffers;
	private int discountOffers;
	
	
	public String getBatchId() {
		return batchId;
	}
	public void setBatchId(String batchId) {
		this.batchId = batchId;
	}
	public int getBatchSize() {
		return batchSize;
	}
	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}
	public int getN0() {
		return n0;   //sum of Resiult = 0
	}
	public void setN0(int n0) {
		this.n0 = n0;
	}
	public int getN1() {
		return batchSize - n0;
	}
	public int getNoOffers() {
		return noOffers;
	}
	public void setNoOffers(int noOffers) {
		this.noOffers = noOffers;
	}
	public int getGiftOffers() {
		return giftOffers;
	}
	public void setGiftOffers(int giftOffers) {
		this.giftOffers = giftOffers;
	}
	public int getDiscountOffers() {
		return discountOffers;
	}
	public void setDiscountOffers(int discountOffers) {
		this.discountOffers = discountOffers;
	}
	
	
	
	
	@Override
	public String toString() {
		return "BatchSummary [batchId=" + batchId + ", batchSize=" + batchSize + ", n0=" + n0 + ", noOffers=" + noOffers
				+ ", giftOffers=" + giftOffers + ", discountOffers=" + discountOffers + "]";
	}
	public static BatchSummary  newBatchSummary(String batchName, Long batchSize, Long zeroCount) {
		BatchSummary result = new BatchSummary();
		result.setBatchId(batchName);
		result.setBatchSize( (int)(long) batchSize);
		result.setN0((int) (long)zeroCount);
		return result;
	}
	
}

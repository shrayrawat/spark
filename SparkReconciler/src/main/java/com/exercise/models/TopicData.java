package com.exercise.models;

import java.util.HashSet;
import java.util.Set;

public class TopicData {
	private Set<String> supplyData;
	private Set<String> demandData;

	public TopicData() {
		this.supplyData = new HashSet<String>();
		this.demandData = new HashSet<String>();
	}

	public void addSupplyRecord(String supplyRec) {
		supplyData.add(supplyRec);
	}

	public void addDemandRecord(String demandRec) {
		demandData.add(demandRec);
	}

	public int countSupply() {
		return supplyData.size();
	}

	public int countDemand() {
		return demandData.size();
	}
}

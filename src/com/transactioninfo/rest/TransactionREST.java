package com.transactioninfo.rest;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.json.JSONObject;

@Path("/")
public class TransactionREST {
	
	public static Map<Long, List<Double>> transactionsData = new ConcurrentHashMap<Long, List<Double>>();
	
	@Path("/statistics")
	@GET
	@Produces("application/json")
	public String getTransactions() {
		
		JSONObject res = new JSONObject();
		
		Double sum = 0.0;
		Double avg = 0.0;
		Double max = 0.0;
		Double minima = 0.0;
		ArrayList<Double> min = new ArrayList<Double>();
		Long count = 0L;
		
		//get epoch time in ms of last minute
		Long lastMinuteTimestamp = (Instant.now().toEpochMilli() - 60000);
		
		//traverse the map and send back all the transactions that have happened in the last 60 seconds
		for (Long timestamp : this.transactionsData.keySet()) {
			
			if (timestamp >= lastMinuteTimestamp) {
				List<Double> tempContainer = this.transactionsData.get(timestamp);
				JSONObject params = returnParams(tempContainer);
				sum = params.getDouble("sum");
				max = (max < params.getDouble("max")) ? params.getDouble("max") : max;
				min.add(params.getDouble("min"));
				count += tempContainer.size();
			}
		}
		
		res.put("sum", sum);
		res.put("max", max);
		minima = (min.size() > 0) ? Collections.min(min) : minima;
		res.put("min", minima);
		res.put("count", count);
		avg = (count > 0L) ? sum/count : 0.0;
		res.put("avg", avg);
		
		//get the last 
		return res.toString();
	}
	
	@Path("/transactions")
	@POST
	@Consumes("application/json")
	@Produces("application/json")
	public String postTransactions(String request) {
		
		JSONObject req = new JSONObject(request);
		JSONObject res = new JSONObject();
		
		Double amount = req.getDouble("amount");
		Long timestamp = req.getLong("timestamp");
		
		//get epoch time in ms of last minute
		Long lastMinuteTimestamp = (Instant.now().toEpochMilli() - 60000);
		
		if (lastMinuteTimestamp < timestamp) {
			
			//two transactions can happen at exactly same time
			if (this.transactionsData.containsKey(timestamp)) {
				List<Double> tempContainer = this.transactionsData.get(timestamp);
				tempContainer.add(amount);
				this.transactionsData.put(timestamp, tempContainer);
			}
			
			else {
				//insert the entry by sorting it
				List<Double> tempContainer = new ArrayList<Double>();
				tempContainer.add(amount);
				this.transactionsData.put(timestamp, tempContainer);
				
			}
			
			res.put("status", 201);
		}
		
		else {
			//transaction is older than sixty seconds
			res.put("status", 204);
		}
		return res.toString();
	}
	
	public JSONObject returnParams(List<Double> tempContainer) {
		
		JSONObject params = new JSONObject();
		Double sum = 0.0;
		Double max = 0.0;
		Double min = tempContainer.get(0);
		
		for (Double value : tempContainer) {
			sum += value;
			max = (max < value) ? value : max;
			min = (min > value) ? value : min;
		}
		
		params.put("sum", sum);
		params.put("max", max);
		params.put("min", min);
		
		return params;
	}
}

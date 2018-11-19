package com.proseeda;

import java.sql.*;
import java.text.SimpleDateFormat;
import com.google.gson.*;
import com.mongodb.*;
import java.util.*;
import java.util.Date;
public class DashboardUtil {

	private static final String AVG_USER = "avgUser";

	public static void main(String[] args) {
		// (A) database connection
				// "jdbc:mysql://localhost:3306/northwind" - the database url of the form jdbc:subprotocol:subname
				// "dbusername" - the database user on whose behalf the connection is being made
				// "dbpassword" - the user's password
				
				// (C) format returned ResultSet as a JSON array
				DashboardUtil db = new DashboardUtil();
				//ziv@proseeda.com, startDate: 5/01/2018, endDate: 5/01/2019
				//01/05/2018, endDate: 01/5/2019
				//05/01/2018, endDate: 5/01/2019
			    db.getUserMonthlyMap("ziv@proseeda.com","05/01/2018","5/01/2019");

	}

	public JsonArray getUserMonthlyMap(String userId,String startDate,String endDate) {
		try {
			System.out.println("i was called");
			MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
			
			DB database = mongoClient.getDB("proseeda");
			System.out.println("i was called2");
			DBCollection collection = database.getCollection("avgactivitiy");
			System.out.println("i was called3");
			
		    //cursorObj = collectionObj.find(selectQuery);
		    //DBCursor cursor = collection.find(selectQuery);
			DBCursor cursor;
			if(userId!=null)
			{
				//db.activities.find({"date" : {$gt : "11/12/2018"}})
				BasicDBObject query = new BasicDBObject();
				query.put("date",new BasicDBObject("$gte", startDate).
						append("$lte", endDate));
				//query.put("date",new BasicDBObject("$lt", endDate));
			    //query.put("userId",userId);
			    
				cursor = collection.find(query);
				
			}
			else
			{
				cursor = collection.find();
			}
			
			
			System.out.println("i was called4");
			JsonArray recordsArray = new JsonArray();
			HashMap map = new HashMap();
			while(cursor.hasNext()){
				
				DBObject jo = (DBObject)cursor.next();
				System.out.println("i was called");
				System.out.println("found" + jo.toString());
				SimpleDateFormat df = new SimpleDateFormat("MM/dd/yyyy");
				
				Date date = df.parse((String)jo.get("date"));
				int month = date.getMonth()+1;
				int year = date.getYear()-100+2000;
				String monthy = new String(month+"/01"+"/"+year);
				int duration =0;
				if(jo.get("Duration") instanceof Double)
				{
					duration = (int)((Double)jo.get("Duration")).doubleValue()*10;
				}
				else
				{
					duration = (int)((Integer)jo.get("Duration")).intValue();
				}
				if(map.containsKey(monthy)) {
					HashMap monthMap = (HashMap)map.get(monthy);
					if(monthMap.containsKey(jo.get("userId")))
					{
						Integer mapDuration = (Integer)monthMap.get(jo.get("userId"));
						int value = mapDuration.intValue()+duration;
						monthMap.put(jo.get("userId"), new Integer(value));
					}
					else {
						monthMap.put(jo.get("userId"),new Integer(duration));
					}
					
				}else {
					HashMap monthMap = new HashMap();
					monthMap.put(jo.get("userId"),new Integer(duration));
					map.put(monthy, monthMap);
				}				
				
				//while (employees.next()) {
					
					
			}
			
			Iterator iter = map.entrySet().iterator();
			while(iter.hasNext()) {
				Map.Entry en = (Map.Entry)iter.next();
				
				JsonObject currentRecord = new JsonObject();
				//currentRecord.add("Name", new JsonPrimitive(userId));
				currentRecord.add("Date",
						new JsonPrimitive((String)en.getKey()));
				HashMap monthMap = (HashMap)en.getValue();
				Iterator monthIter = monthMap.entrySet().iterator();
				int currentUserDuration=0;
				int avgUserDuration=0;
				int userCount=0;
				while(monthIter.hasNext()) {
					Map.Entry enMonth = (Map.Entry)monthIter.next();
					Integer duration = (Integer)enMonth.getValue();
					if(enMonth.getKey().equals(userId))
					{	
						currentUserDuration += duration.intValue();
					}
					else
					{
						userCount++;
						avgUserDuration += duration.intValue();
					}
				}
				currentRecord.add("Current User",
						new JsonPrimitive(Integer.toString(currentUserDuration)));
				currentRecord.add("Avrage User",
						new JsonPrimitive(Integer.toString(avgUserDuration/userCount)));
				
				recordsArray.add(currentRecord);
			}
			// (D)
			System.out.println("DashboardUtil:: here is what we have ");
			System.out.println(recordsArray.toString());
			return recordsArray;
			//out.print(recordsArray);
			//out.flush();
		}catch(Exception e) {
			e.printStackTrace();
		}
		return null;
	}

}



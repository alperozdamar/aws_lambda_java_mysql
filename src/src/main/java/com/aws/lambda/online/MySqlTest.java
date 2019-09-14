package com.aws.lambda.online;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.aws.lambda.online.data.RequestDetails;
import com.aws.lambda.online.data.ResponseDetails;


public class MySqlTest implements RequestHandler<RequestDetails, ResponseDetails>
{
	
	public static LambdaLogger logger;
   		
	public ResponseDetails handleRequest(RequestDetails requestDetails, Context context) {
		logger = context.getLogger();
		
		//logger.log("Lambda function is invoked:"+requestDetails.getEmpID() + "," + requestDetails.getEmpName());
		
		// TODO Auto-generated method stub
		ResponseDetails responseDetails = new ResponseDetails();
				try {
					insertDetails(requestDetails, responseDetails);
				} catch (SQLException sqlException) {
					responseDetails.setMessageID("999");
					responseDetails.setMessageReason("Unable to Register "+sqlException);
				}
				return responseDetails;
	}

	private void insertDetails(RequestDetails requestDetails, ResponseDetails responseDetails) throws SQLException {
		Connection connection=null;
		Statement statement=null;
		try {
			 connection = getConnection();
			 statement = connection.createStatement();
			String query = getquery(requestDetails);
			int responseCode = statement.executeUpdate(query);
			if(1 == responseCode)
			{
				responseDetails.setMessageID(String.valueOf(responseCode));
				responseDetails.setMessageReason("Successfully updated details");
			}
			statement.close();
			connection.close();
		} catch (Exception e) {
			e.printStackTrace();
			statement.close();
			connection.close();
		}		
	}

	private String getquery(RequestDetails requestDetails) {
		
//		String query = "INSERT INTO testdb.employee(empid, empname) VALUES (";
//		if (requestDetails != null) {
//			query = query.concat("'" + requestDetails.getEmpID() + "','" 
//					+ requestDetails.getEmpName() + "')");
//		}
		
		
		String query = "INSERT INTO testdb.employee(empname) VALUES (";
		if (requestDetails != null) {
			query = query.concat("'"+ requestDetails.getEmpName() + "')");
		}
		
		//System.out.println("the query is "+query);
		return query;
	}

	private Connection getConnection() throws SQLException {
        
		//logger.log("Waiting to GetConnection.....");
		
		// TODO Auto-generated method stub
				String url = "jdbc:mysql://mysqltest.cqfxlfeycbgd.us-west-2.rds.amazonaws.com:3306";
				String username = "root";
				String password = "alper123";
				
				
				
				Connection conn = DriverManager.getConnection(url, username, password);
				
				//logger.log("Connection established.....");
				
				return conn;
	}

}

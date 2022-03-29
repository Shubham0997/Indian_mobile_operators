package Assignment3.Indian_mobile_operators;

/**
We have different mobile operators in India.
We have to create DB to store Operators details.
We need to store LINE RANGE data( e.g. we have to define range for each operator e.g. 98720***** - 98729***** belongs to Airtel, 98140***** to 98149***** means Idea etc...)
Then we have to define Regions for these ranges too i.e. this RANGE belongs to Punjab airtel and this range belongs to Haryana Airtel.
As of now , we can assume there is no concept of MNP i.e. Mobile number Portability.
We have to store all SMSs those sent from any Indian mobile number to any Indian mobile number.
Other message details to be store :From,To,From Operator,To Operator,sent time, received time,deliveryStatus.
Queries :-
We have to print all messages sent from *** number To any number.
We have to print all messages received by *** from any number.
We have to print all messages sent from XXXX to YYYY.
We have to print all messages received by *** from Punjab number.
We have to print all messages received by *** from Airtel Punjab number.
We have to print all messages received from 98786912** ( here ** mean it can by any two digit, i.e. messages from 9878691291,92,93,94,95 etc..) by ***.
We have to print all messages those were sent from Punjab number but FAILED.
You have to design database and then write Java code to fullfil all the requirements.
 *
 * @author Shubham Sharma
 *
 *
 **/

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;


public class IndianMobileOperators {


	private static Logger log = LogManager.getLogger(IndianMobileOperators.class.getName()); // Logger Method Object
	static Connection connection = null;
	

	public static void main(String[] args) throws Exception {
		try {
			// initialize resources
			initializeResource();
			 // read url value from config.properties
			String url = ResourceInitializer.getResourceValue("url");
			// read user value from config.properties
			String username = ResourceInitializer.getResourceValue("user"); 
			 // read password value from config.properties
			String password = ResourceInitializer.getResourceValue("password");

		
			Connection connection = DriverManager.getConnection(url, username, password);
			
			// check for Table existence, create and populate if not found.
			checkTableExistence(connection);
			
			// execute the queries as per the assignment
			executeAssignmentQueries(connection); 

			
		} catch (Exception e) {
			log.fatal("Exception occured : " + e);
		}finally {
			if(connection != null) {
				connection.close();
			}
			

	}
	}


	public static void initializeResource() throws Exception {
		try {
			ResourceInitializer.initializeFile();
		} catch (Exception e) {
			throw new Exception("Config not loaded");
		}
	}
		
	
	
	/**
	 * @param connection : Connection to the server
	 * @param databaseName 
	 * @throws Exception
	 *  following function checks for tables existence, if doesn't exist, it will create and further call functions to populate them
	 */
	public static void checkTableExistence(Connection connection) throws Exception {
		
		try{
			
			//fetching database metadata
			DatabaseMetaData databaseMetadata = connection.getMetaData(); 
			
			// check if "mobile_operators" table exists
			log.info("Checking if 'mobile_operators' table exists");
			ResultSet tables = databaseMetadata.getTables(null, null, "mobile_operators", null);
			if (tables.next()) {
				log.info("Table 'mobile_operators' exists");
			}
			else {
				log.info("Table 'mobile_operators' doesn't exists, creating...");
				createMobileOperatorTable(connection);
			}
	
			//checking if 'messages' table exists
			log.info("Checking if 'messages' table exists");
			tables = databaseMetadata.getTables(null, null, "messages", null);
			if (tables.next()) {
				log.info("Table 'mobile_operators' exists");
			}
			else {
				log.info("Table 'mobile_operators' doesn't exists");
				createMessagesTable(connection);
			}
			
	
		}catch(Exception e) {
			throw new Exception(e+ "	Exception occured while checking/creating database");
		}
		
	}
	
	

	/**
	 * 
	 * @param connection : Connection to the database
	 * @throws Exception
	 * this method create 'mobile_operators' table and further call for method to populate it
	 */
	public static void createMobileOperatorTable(Connection connection) throws Exception {
		
			try(Statement statement = connection.createStatement();){

			log.info("Creating mobile_operators table"); //creating mobile_oeprators table which will contain mobile operator data
			String sql = "CREATE TABLE mobile_operators " + "(Ranges INTEGER, " + " Operators VARCHAR(255), "
					+ " Region VARCHAR(255), " + " PRIMARY KEY ( Ranges ))";
			statement.execute(sql);

			//populating data table which will contain mobile operator data, i.e ranges, operators, regions
			populateOperatorDataTable(connection);
			
		}catch(Exception e) {
			throw new Exception(e+ "	Exception occured while creating tables");
		}
	}
	
	
	/**
	 * @param connection : Connection to the database
	 * @throws Exception
	 * This will create 'messages' table and further call for method to populate it
	 */
	public static void createMessagesTable(Connection connection) throws Exception {
		try  {
			Statement statement = connection.createStatement();

		
			log.info("Creating tables messages"); //creating messages table which will contain messages and their details.
			String sql = "CREATE TABLE messages " + "(sms_From BIGINT, " + " sms_to BIGINT, " + " message VARCHAR(255),"
					+ " from_Operators VARCHAR(255), " + " from_region VARCHAR(255), " + " to_Operators VARCHAR(255), "
					+ " to_region VARCHAR(255)," + " sent_time DATETIME," + " received_time DATETIME,"
					+ " delivery_status VARCHAR(255) " + " )";
			statement.execute(sql);

			//populating data table which will contain mobile operator data, i.e ranges, operators, regions
			populateMessages(connection);  //comment this line and uncomment //populateMessagesTable(connection); if inputs are needed to be taken in console. 
			//populateMessagesTable(connection); //comment this line and uncomment //populateMessages(connection); if pre-defined inputs are required. 
			
		}catch(Exception e) {
			throw new Exception(e+ "	Exception occured while creating tables");
		}
	}
	
	
	/**
	 * 
	 * @param connection : connection to the database
	 * @throws Exception
	 * below method populates mobile_operators table with operator information
	 */
	public static void populateOperatorDataTable(Connection connection) throws Exception {
		// String array containing region names to be entered.
		String[] regions = new String[] { "Punjab", "Himachal Pradesh", "Delhi", "Haryana", "Uttarakhand",
				"Uttar Pradesh", "Rajasthan", "Maharashtra", "Karnataka", "Gujarat" }; 

		try (PreparedStatement ps = connection.prepareStatement("insert into mobile_operators values(?,?,?)");) {

			// code below populates mobile_operators table with operator information
			log.info("Adding Airtel, Idea, Jio, BSNL Information...");
			int airtelRange = 98720;
			int ideaRange = 98140;
			int jioRange = 70180;
			int bsnlRange = 98780;

			for (int i = 0; i < regions.length; i++) {

				// adding airtel range
				ps.setInt(1, airtelRange);
				ps.setString(2, "Airtel");
				ps.setString(3, regions[i]);
				ps.addBatch();
				airtelRange++;

				// adding Idea range
				ps.setInt(1, ideaRange);
				ps.setString(2, "Idea");
				ps.setString(3, regions[i]);
				ps.addBatch();
				ideaRange++;

				// adding Jio range
				ps.setInt(1, jioRange);
				ps.setString(2, "Jio");
				ps.setString(3, regions[i]);
				ps.addBatch();
				jioRange++;

				// adding BSNL range
				ps.setInt(1, bsnlRange);
				ps.setString(2, "BSNL");
				ps.setString(3, regions[i]);
				ps.addBatch();
				bsnlRange++;
			}

			ps.executeBatch();
			log.info("Table mobile_operators populated");

		
		} catch (Exception e) {
			throw new Exception(e+ "	Exception occured while populating mobile_operator tables");
		}
	}
	
	
	
	/**
	 * @param connection : connection to the database
	 * @throws Exception
	 * below method populates messages table with pre-defined values
	 */
	public static void populateMessages(Connection connection) throws Exception {
		log.info("Inserting messages into messages table");
		
		try(Statement statement= connection.createStatement();){

		statement.addBatch("insert into messages values(9872900001, 9814900001,'This is message 2',(select operators from mobile_operators where ranges=98729),(select region from mobile_operators where ranges=98729 ),(select operators from mobile_operators where ranges=98149),(select region from mobile_operators where ranges=98149),CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'Received')");
		statement.addBatch("insert into messages values(7018648324, 7018938283,'This is message 1',(select operators from mobile_operators where ranges=70186),(select region from mobile_operators where ranges=70186),(select operators from mobile_operators where ranges=70189),(select region from mobile_operators where ranges=70189),CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'Received')");
		statement.addBatch("insert into messages values(9814183756, 9872623947,'This is message 3',(select operators from mobile_operators where ranges=98141),(select region from mobile_operators where ranges=98141),(select operators from mobile_operators where ranges=98726),(select region from mobile_operators where ranges=98726),CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'Received')");
		statement.addBatch("insert into messages values(9814034342, 7018648324,'This is message 4',(select operators from mobile_operators where ranges=98140),(select region from mobile_operators where ranges=98140),(select operators from mobile_operators where ranges=70186),(select region from mobile_operators where ranges=70186),CURRENT_TIMESTAMP,NULL,'Failed')");
		statement.addBatch("insert into messages values(9878691251, 9814538238,'This is message 5',(select operators from mobile_operators where ranges=98786),(select region from mobile_operators where ranges=98786),(select operators from mobile_operators where ranges=98145),(select region from mobile_operators where ranges=98145),CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'Received')");
		statement.addBatch("insert into messages values(9872930493, 9878349412,'This is message 6',(select operators from mobile_operators where ranges=98729),(select region from mobile_operators where ranges=98729),(select operators from mobile_operators where ranges=98783),(select region from mobile_operators where ranges=98783),CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'Received')");
		statement.addBatch("insert into messages values(7018648324, 9878349412,'This is message 7',(select operators from mobile_operators where ranges=70186),(select region from mobile_operators where ranges=70186),(select operators from mobile_operators where ranges=98783),(select region from mobile_operators where ranges=98783),CURRENT_TIMESTAMP,NULL,'Failed')");
		statement.addBatch("insert into messages values(9872727211, 9814902322,'This is message 8',(select operators from mobile_operators where ranges=98727),(select region from mobile_operators where ranges=98727),(select operators from mobile_operators where ranges=98149),(select region from mobile_operators where ranges=98149),CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'Received')");
		statement.addBatch("insert into messages values(9878593493, 7018012820,'This is message 9',(select operators from mobile_operators where ranges=98785),(select region from mobile_operators where ranges=98785),(select operators from mobile_operators where ranges=70180),(select region from mobile_operators where ranges=70180),CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'Received')");
		statement.addBatch("insert into messages values(9814723923, 7018723923,'This is message 10',(select operators from mobile_operators where ranges=98147),(select region from mobile_operators where ranges=98147),(select operators from mobile_operators where ranges=70187),(select region from mobile_operators where ranges=70187),CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'Received')");
		statement.addBatch("insert into messages values(9878723484, 7018937289,'This is message 11',(select operators from mobile_operators where ranges=98787),(select region from mobile_operators where ranges=98787),(select operators from mobile_operators where ranges=70189),(select region from mobile_operators where ranges=70189),CURRENT_TIMESTAMP,NULL,'Failed')");
		statement.addBatch("insert into messages values(7018648324, 9814434934,'This is message 12',(select operators from mobile_operators where ranges=70186),(select region from mobile_operators where ranges=70186),(select operators from mobile_operators where ranges=98144),(select region from mobile_operators where ranges=98144),CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'Received')");
		statement.addBatch("insert into messages values(7018648324, 9814434934,'This is message 13',(select operators from mobile_operators where ranges=70186),(select region from mobile_operators where ranges=70186),(select operators from mobile_operators where ranges=98144),(select region from mobile_operators where ranges=98144),CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'Received')");
		statement.addBatch("insert into messages values(7018648324, 9814434934,'This is message 14',(select operators from mobile_operators where ranges=70186),(select region from mobile_operators where ranges=70186),(select operators from mobile_operators where ranges=98144),(select region from mobile_operators where ranges=98144),CURRENT_TIMESTAMP,NULL,'Failed')");
		statement.addBatch("insert into messages values(7018648324, 9814434934,'This is message 15',(select operators from mobile_operators where ranges=70186),(select region from mobile_operators where ranges=70186),(select operators from mobile_operators where ranges=98144),(select region from mobile_operators where ranges=98144),CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'Received')");
		statement.addBatch("insert into messages values(9872034342, 7018648324 ,'This is message 16',(select operators from mobile_operators where ranges=98720),(select region from mobile_operators where ranges=98720),(select operators from mobile_operators where ranges=70186),(select region from mobile_operators where ranges=70186),CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'Received')");
		
		statement.executeBatch();
		}catch(Exception e) {
			throw new Exception(e+ "	Exception occured while populating messages tables");
		}
	}
	
	

	/**
	 * @param connection : connection to the database
	 * @throws Exception
	 * below method populates messages table by taking user inputs
	 */
	public static void populateMessagesTable(Connection connection) throws Exception {
		
		try(PreparedStatement ps=connection.prepareStatement("insert into messages values(?,?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,?)");){
		
		Statement statement= connection.createStatement();
		
		BufferedReader br=new BufferedReader(new InputStreamReader(System.in));  
		
		log.info("Enter the number of messages : ");
		int numberOfMessages = Integer.parseInt(br.readLine());  // reading number of messages
		
		long senderNumber;
		long receiverNumber;
		String message;
		String deliveryStatus;
		
		//Entering messages by taking input in the console
		for(int i=1;i<=numberOfMessages;i++) {
			
			log.info("Enter the Sender's phone number : ");
			senderNumber= Long.parseLong(br.readLine());
			
			log.info("Enter the Receiver's phone number : ");
			receiverNumber= Long.parseLong(br.readLine());
			
			log.info("Enter the message : ");
			message = br.readLine();  
		
			//split the phone number to get first 5 character to check the range an operator of the numbers
			String sendersNumber= String.valueOf(senderNumber);
			sendersNumber=sendersNumber.substring(0, (sendersNumber.length()/2));
			
			String receiversNumber=String.valueOf(receiverNumber);
			receiversNumber=receiversNumber.substring(0, (receiversNumber.length()/2));
			
			//fetching the operator and region of the sender
			ResultSet rs= statement.executeQuery("select operators,region from mobile_operators where ranges="+sendersNumber+"");
			rs.next();
			String operator1=rs.getString(1);
			String region1=rs.getString(2);
			
			//fetching the operator and region of the receiver
			ResultSet rs2= statement.executeQuery("select operators,region from mobile_operators where ranges="+receiversNumber+"");
			rs2.next();
			String operator2=rs2.getString(1);
			String region2=rs2.getString(2);
	
			
			log.info("Enter the delivery status : ");
			deliveryStatus = br.readLine();  

			//setting fields for prepared statement
			ps.setLong(1, senderNumber);
			ps.setLong(2, receiverNumber);
			ps.setString(3, message);
			ps.setString(4, operator1);
			ps.setString(5, region1);
			ps.setString(6, operator2);
			ps.setString(7, region2);
			ps.setString(8, deliveryStatus);
			
			ps.addBatch();
			log.info("**********************");
		}
		
		ps.executeBatch(); 
		log.info("Records added successfuly");  
		
		//setting received_time as NULL for failed delivery_status 
		int result=statement.executeUpdate("UPDATE messages SET received_time=NULL where delivery_status='Failed'");  
		log.info(result+" records affected"); 
		
		}catch(Exception e) {
			throw new Exception(e+ "	Exception occured while populating messages tables");
		}
		
	}
	

	
	/**
	 * @param connection : connection to the server
	 * @param databaseName
	 * @throws Exception
	 * following method executes the queries in the assignment 
	 */
	public static void executeAssignmentQueries(Connection connection) throws Exception {
		
//			Statement statement = connection.createStatement();
//
//			String selectDatabase = "USE " + databaseName; //selecting database
//		    statement.executeUpdate(selectDatabase);
//		    log.info("Database selected successfully");   
//		    log.info("**************************************");
//		    
//		    
//			// print messages from a given number
//			log.info("Query 1: Print all messages sent from a given number. ");
//			String query = "Select message from messages where sms_from=7018648324";
//			ResultSet rs = statement.executeQuery(query);
//			while (rs.next()) {
//				log.info("Message : " + rs.getString("message"));
//			}
//
//			log.info("**************************************");
//
//			//print messages to a given number
//			log.info("Query 2: Print all messages to a given number. ");
//			query = "Select message from messages where sms_to=9878349412";
//			rs = statement.executeQuery(query);
//			while (rs.next()) {
//				log.info("Message : " + rs.getString("message"));
//			}
//
//			log.info("**************************************");
//
//			//Print all messages from a given number to a given number
//			log.info("Query 3: Print all messages sent between two years. ");
//			query = "Select message from messages where YEAR(sent_time) BETWEEN 2021 AND 2022";
//			rs = statement.executeQuery(query);
//			while (rs.next()) {
//				log.info("Message : " + rs.getString("message"));
//			}
//
//			log.info("**************************************");
//
//			// Print all messages received by given number from punjab number
//			log.info("Query 4: Print all messages receieved by given number from punjab number. ");
//			query = "Select message from messages where sms_to=7018648324 AND from_Region='Punjab'";
//			rs = statement.executeQuery(query);
//			while (rs.next()) {
//				log.info("Message : " + rs.getString("message"));
//			}
//
//			log.info("**************************************");
//
//			//Print all messages received by given number from airtel punjab number.
//			log.info("Query 5: Print all messages receieved by given number from airtel punjab number. ");
//			query = "Select message from messages where sms_to=7018648324 AND from_Region='Punjab' AND from_Operators='Airtel'";
//			rs = statement.executeQuery(query);
//			while (rs.next()) {
//				log.info("Message : " + rs.getString("message"));
//			}
//
//			log.info("**************************************");
//
//			//Print all messages sent by 98786912**
//			log.info("Query 6: Print all messages sent by 98786912** . ");
//			query = "Select message from messages where sms_from>9878691200 AND sms_from<=9878691299";
//			rs = statement.executeQuery(query);
//			while (rs.next()) {
//				log.info("Message : " + rs.getString("message"));
//			}
//
//			log.info("**************************************");
//
//			//Print all messages sent from punjab but failed .
//			log.info("Query 7: Print all messages sent from punjab but failed . ");
//			query = "Select message from messages where from_region='Punjab' AND delivery_status='Failed'";
//			rs = statement.executeQuery(query);
//			while (rs.next()) {
//				log.info("Message : " + rs.getString("message"));
//			}
//
//			log.info("**************************************");
//			
//			
			
			try(Scanner sc = new Scanner(System.in);){
			int userChoice;
			
			do {
			
				log.info("Enter the number with respect to the operation : ");
				log.info("***************************************************************************************");
				log.info("(1) Enter 1 to Print all messages sent from a given number. ");
				log.info("(2) Enter 2 to Print all messages to a given number. ");
				log.info("(3) Enter 3 to Print all messages sent between two years. ");
				log.info("(4) Enter 4 to Print all messages receieved by given number from punjab number. ");
				log.info("(5) Enter 5 to Print all messages receieved by given number from airtel punjab number. ");
				log.info("(6) Enter 6 to Print all messages sent by 98786912**, (Where ** could be any two digits). ");
				log.info("(7) Enter 7 to Print all messages sent from punjab but failed . ");
				log.info("(8) Enter 0 to exit. ");
				log.info("***************************************************************************************");
				userChoice = sc.nextInt(); 
				
				if(userChoice==1) {

					messagesSentFromNumber();
				}else if(userChoice ==2) {
					
					messagesSentToNumber();
				}else if(userChoice ==3) {
					
					messageSentBetweenTwoYears();
				}else if(userChoice ==4) {
					
					 messageReceivedFromPunjab();
				}else if(userChoice ==5) {
					
					messageReceivedFromAirtelPunjab();
				}else if(userChoice ==6) {
					
					messageSentBy98786912();
				}else if(userChoice ==7) {
					
					messageFromPunjabFailed();
				}else if(userChoice ==0) {
					log.info("You have entered 0, Exiting");
					break;
				}else {
					log.info("You have entered "+ userChoice+ " which is a wrong input.");
				}
				
			}while(true); 
				
		

		}catch(Exception e) {
			log.fatal(e+ "	Exception has occured while executing assignment queries");
		}
	}
	
	public static void messagesSentFromNumber() {
		log.info("You have entered 1");
	}
	public static void messagesSentToNumber() {
		log.info("You have entered 2");
	}
	public static void messageSentBetweenTwoYears() {
		log.info("You have entered 3");
	}
	public static void messageReceivedFromPunjab() {
		log.info("You have entered 4");
	}
	public static void messageReceivedFromAirtelPunjab() {
		log.info("You have entered 5");
	}
	public static void messageSentBy98786912() {
		log.info("You have entered 6");
	}
	public static void messageFromPunjabFailed() {
		log.info("You have entered 7");
	}
	
	
}
		



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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
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

			//getting connection to the server
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
			
			// check if "operator_range" table exists
			log.info("Checking if 'operator_range' table exists");
			ResultSet tables = databaseMetadata.getTables(null, null, "operator_range", null);
			if (tables.next()) {
				log.info("Table 'operator_range' exists");
			} else {
				log.info("Table 'operator_range' doesn't exists.");
				createOperatorRangeTable(connection);
			}

			// check if "operator_region" table exists
			log.info("Checking if 'operator_region' table exists");
			tables = databaseMetadata.getTables(null, null, "operator_region", null);
			if (tables.next()) {
				log.info("Table 'operator_region' exists");
			}
			else {
				log.info("Table 'operator_region' doesn't exists.");
				createOperatorRegionTable(connection);
			}
	
			
			//checking if 'messages' table exists
			log.info("Checking if 'messages' table exists");
			tables = databaseMetadata.getTables(null, null, "messages", null);
			if (tables.next()) {
				log.info("Table 'messages' exists");
			}
			else {
				log.info("Table 'messages' doesn't exists");
				createMessagesTable(connection);
			}
			
		}catch(Exception e) {
			throw new Exception(e+ "	Exception occured while checking/creating tables");
		}
		
	}
	
	/**
	 * 
	 * @param connection : Connection to the database
	 * @throws Exception
	 * this method create 'mobile_operators' table and further call for method to populate it
	 */
	public static void createOperatorRangeTable(Connection connection) throws Exception {
		
			try(Statement statement = connection.createStatement();){

			log.info("Creating operator_range table"); //creating operator_range table which will contain mobile operator ranges
			String sql = "CREATE TABLE operator_range " + "(Ranges INTEGER, " + "Operators VARCHAR(255), " + " PRIMARY KEY ( Ranges ))";
			statement.execute(sql);

			//populating operator_range table which will contain operator_ranges with ID
			populateOperatorRangeTable(connection);
			
		}catch(Exception e) {
			throw new Exception(e+ "	Exception occured while creating tables");
		}
	}
	
	
	/**
	 * 
	 * @param connection : Connection to the database
	 * @throws Exception
	 * this method create 'operator_region' table and further call for method to populate it
	 */
	public static void createOperatorRegionTable(Connection connection) throws Exception {
		
			try(Statement statement = connection.createStatement();){

			log.info("Creating operator_region table"); //creating operator_region table which will contain mobile operator data
			String sql = "CREATE TABLE operator_region " + "(Region_code INTEGER, " + " Region VARCHAR(255), " + " PRIMARY KEY ( Region_code ))";
			statement.execute(sql);

			//populating operator_region table which will contain operator regions
			populateOperatorRegionTable(connection);
			
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
					+ " sent_time DATETIME," + " received_time DATETIME,"
					+ " delivery_status VARCHAR(255) " + " )";
			statement.execute(sql);

			//populating messages table
			populateMessages(connection); 
			
		}catch(Exception e) {
			throw new Exception(e+ "	Exception occured while creating tables");
		}
	}
	
	
	/**
	 * 
	 * @param connection : connection to the database
	 * @throws Exception
	 * below method populates operator_range table with operator ranges
	 */
	public static void populateOperatorRangeTable(Connection connection) throws Exception {
	
		
		try (PreparedStatement preparedStatement = connection.prepareStatement("insert into operator_range values(?,?)");) {
			//operatorMap contains ranges of mobile operators
			 HashMap<Integer, String> operatorMap = new HashMap<>();
			 operatorMap.put(9872, "Airtel");
			 operatorMap.put(9814, "Idea");
			 operatorMap.put(7018, "Jio");
			 operatorMap.put(9878, "BSNL");
			 
				// code below populates mobile_operators table with operator information
				log.info("Adding Airtel, Idea, Jio, BSNL Information...");
			 for (Map.Entry<Integer, String> entry : operatorMap.entrySet()) {

				 preparedStatement.setInt(1, entry.getKey()); //entering range
				 preparedStatement.setString(2, entry.getValue()); //entering operators
				 preparedStatement.addBatch();

				}

			 preparedStatement.executeBatch();
			log.info("Table operator_range populated");

		
		} catch (Exception e) {
			throw new Exception(e+ "	Exception occured while populating operator_range tables");
		}
	}
	
	/**
	 * 
	 * @param connection : connection to the database
	 * @throws Exception
	 * below method populates operator_region table with operator regions
	 */
	public static void populateOperatorRegionTable(Connection connection) throws Exception {
		// String array containing region names to be entered.
		String[] regions = new String[] { "Punjab", "Himachal Pradesh", "Delhi", "Haryana", "Uttarakhand",
				"Uttar Pradesh", "Rajasthan", "Maharashtra", "Karnataka", "Gujarat" }; 

		try (PreparedStatement ps = connection.prepareStatement("insert into operator_region values(?,?)");) {

			// code below populates mobile_operators table with operator information
		
			for(int i=0;i<regions.length;i++) {
				ps.setInt(1, i);
				ps.setString(2, regions[i]);
				ps.addBatch();
			}
			
			ps.executeBatch();
			log.info("Table operator_region populated");

		
		} catch (Exception e) {
			throw new Exception(e+ "	Exception occured while populating operator_region tables");
		}
	}
	
	
	
	/**
	 * @param connection : connection to the database
	 * @throws Exception
	 * below method populates messages table with pre-defined values
	 */
	public static void populateMessages(Connection connection) throws Exception {
		
		//arraylist containing insert queries for populating messages table.
		ArrayList<String> insertQueries = new ArrayList<String>(); 
		insertQueries.add("insert into messages values(9872900001, 9814900001,'This is message 2',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'Received')");
		insertQueries.add("insert into messages values(9872900001, 9814900001,'This is message 2',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'Received')");
		insertQueries.add("insert into messages values(7018648324, 7018938283,'This is message 1',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'Received')");
		insertQueries.add("insert into messages values(9814183756, 9872623947,'This is message 3',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'Received')");
		insertQueries.add("insert into messages values(9814034342, 7018648324,'This is message 4',CURRENT_TIMESTAMP,NULL,'Failed')");
		insertQueries.add("insert into messages values(9878691251, 9814538238,'This is message 5',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'Received')");
		insertQueries.add("insert into messages values(9872930493, 9878349412,'This is message 6',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'Received')");
		insertQueries.add("insert into messages values(7018648324, 9878349412,'This is message 7',CURRENT_TIMESTAMP,NULL,'Failed')");
		insertQueries.add("insert into messages values(9872727211, 9814902322,'This is message 8',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'Received')");
		insertQueries.add("insert into messages values(9878593493, 7018012820,'This is message 9',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'Received')");
		insertQueries.add("insert into messages values(9814723923, 7018723923,'This is message 10',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'Received')");
		insertQueries.add("insert into messages values(9878723484, 7018937289,'This is message 11',CURRENT_TIMESTAMP,NULL,'Failed')");
		insertQueries.add("insert into messages values(7018648324, 9814434934,'This is message 12',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'Received')");
		insertQueries.add("insert into messages values(7018648324, 9814434934,'This is message 13',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'Received')");
		insertQueries.add("insert into messages values(7018648324, 9814434934,'This is message 14',CURRENT_TIMESTAMP,NULL,'Failed')");
		insertQueries.add("insert into messages values(7018648324, 9814434934,'This is message 15',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'Received')");
		insertQueries.add("insert into messages values(9872034342, 7018648324 ,'This is message 16',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'Received')");
		log.info("Inserting messages into messages table");
		
		try(Statement statement= connection.createStatement();){

			//adding insert queries into the batch one by one from arraylist
			for(int i=0;i<insertQueries.size();i++) {
				statement.addBatch(insertQueries.get(i));
			}
		
		statement.executeBatch();
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
		
			Statement statement = connection.createStatement();
			BufferedReader reader=new BufferedReader(new InputStreamReader(System.in));  
			try(Scanner scReader = new Scanner(System.in);){
			int userChoice;
			boolean infiniteLoop=true;
			
			//infinite loop which will break if userChoice==0, otherwise will execute queries according to the selected number
			do {
				log.info("***************************************************************************************");
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
				userChoice = scReader.nextInt(); 
				
				switch (userChoice) {

				case 1:
					messagesSentFromNumber(connection, statement, reader);
					break;
				case 2:
					messagesSentToNumber(connection, statement, reader);
					break;
				case 3:
					messageSentBetweenTwoYears(connection, statement, scReader);
					break;
				case 4:
					messageReceivedFromPunjab(connection, statement, reader);
					break;
				case 5:
					messageReceivedFromAirtelPunjab(connection, statement, reader);
					break;
				case 6:
					messageSentByNumber(connection, statement, reader, scReader);
					break;
				case 7:
					messageFromPunjabFailed(connection, statement, reader);
					break;
				case 0:
					log.info("You have entered 0, execution ended.");
					infiniteLoop = false;
					break;
				default:
					log.info("You have entered " + userChoice + " which is a wrong input.");
				}

			}while(infiniteLoop==true); 
				
			

		}catch(Exception e) {
			log.fatal(e+ "	Exception has occured while executing assignment queries");
		}
	}
	
	
	/**
	 * 
	 * @param connection 
	 * @param statement 
	 * @param reader
	 * @throws Exception
	 * this function fetches messages sent from a number
	 */
	public static void messagesSentFromNumber(Connection connection, Statement statement,BufferedReader reader) throws Exception {
		log.info("You have entered 1");
		log.info("Enter the sender's number : ");
		//sample input= 7018648324
		long sender=Long.parseLong(reader.readLine());
		
		String query = "Select message from messages where sms_from="+sender;
		ResultSet rs = statement.executeQuery(query);
		displayMessages(rs);
	}
	
	/**
	 * 
	 * @param connection
	 * @param statement
	 * @param reader
	 * @throws Exception
	 * This function fetches messages received by a number
	 */
	public static void messagesSentToNumber(Connection connection, Statement statement,BufferedReader reader) throws Exception {
		log.info("You have entered 2");
		log.info("Enter receiver's number : ");
		//sample input= 7018648324
		long receiver=Long.parseLong(reader.readLine());
		
		String query = "Select message from messages where sms_to="+receiver;
		ResultSet rs = statement.executeQuery(query);
		displayMessages(rs);
	}
	
	/**
	 * 
	 * @param connection
	 * @param statement
	 * @param reader
	 * @throws Exception
	 * This function fetches messages sent between 2 years
	 */
	public static void messageSentBetweenTwoYears(Connection connection, Statement statement,Scanner reader) throws Exception{
		log.info("You have entered 3");
		//sample input: 2021 2022
		log.info("Enter year 1 : ");
		int year1= reader.nextInt();
		log.info("Enter year 2 : ");
		int year2= reader.nextInt();
		
		String query = "Select message from messages where YEAR(sent_time) BETWEEN "+year1+ " AND " +year2;
		ResultSet rs = statement.executeQuery(query);
		displayMessages(rs);
	}
	
	/**
	 * 
	 * @param connection
	 * @param statement
	 * @param reader
	 * @throws Exception
	 * This function fetches messages sent from punjab
	 */
	public static void messageReceivedFromPunjab(Connection connection, Statement statement,BufferedReader reader) throws Exception{
		log.info("You have entered 4");
		log.info("Enter receiver's number : ");
		//sample input : 7018648324
		long receiver=Long.parseLong(reader.readLine());
		String query = "Select a.message from messages a inner join operator_region b on substring(a.sms_from,5,1) = b.region_code where a.sms_to="+receiver+" and b.region='Punjab'";
		ResultSet rs = statement.executeQuery(query);
		displayMessages(rs);
	}
	
	/**
	 * 
	 * @param connection
	 * @param statement
	 * @param reader
	 * @throws Exception
	 * This function fetches messages sent from punjab and airtel
	 */
	public static void messageReceivedFromAirtelPunjab(Connection connection, Statement statement,BufferedReader reader) throws Exception{
		log.info("You have entered 5");
		log.info("Enter receiver's number : ");
		//sample input : 7018648324
		long receiver=Long.parseLong(reader.readLine());
		String query = "Select a.message from messages a inner join operator_region b inner join operator_range c on "
				+"substring(a.sms_from,5,1) = b.region_code and substring(a.sms_from,1,4)=c.ranges where a.sms_to="+receiver+" and b.region='Punjab' and c.operators='Airtel'";
		ResultSet rs = statement.executeQuery(query);
		displayMessages(rs);
	}
	
	/**
	 * 
	 * @param connection
	 * @param statement
	 * @param reader
	 * @param scReader
	 * @throws Exception
	 * This function fetches message sent by 98786912 followed by two digit user inputs and received by a number
	 */
	public static void messageSentByNumber(Connection connection, Statement statement,BufferedReader reader,Scanner scReader) throws Exception{
		log.info("You have entered 6");
		//sample input: 51 , 9814538238
		log.info("Enter sender's last 2 digits : ");
		int twoDigits=scReader.nextInt();
		log.info("Enter receiver's number : ");
		long receiver=Long.parseLong(reader.readLine());
		
		String query = "Select message from messages where sms_from=98786912"+twoDigits+" AND sms_to="+receiver;
		ResultSet rs = statement.executeQuery(query);
		displayMessages(rs);
	}
	
	/**
	 * 
	 * @param connection
	 * @param statement
	 * @param reader
	 * @throws Exception
	 * This function fetches messages sent from punjab and failed
	 */
	public static void messageFromPunjabFailed(Connection connection, Statement statement,BufferedReader reader) throws Exception{
		log.info("You have entered 7");
		String query = "Select a.message from messages a inner join operator_region b on substring(a.sms_from,5,1) = b.region_code where b.region='Punjab' and a.delivery_status='Failed'";
		ResultSet rs = statement.executeQuery(query);
		displayMessages(rs);

	}
	
	/**
	 * 
	 * @param queryResult
	 * @throws Exception
	 * This function displays messages, fetched by queries
	 */
	public static void displayMessages(ResultSet queryResult) throws Exception {
		while (queryResult.next()) {
			log.info("Message : " + queryResult.getString("message"));
		}
	}
	
	
}
		



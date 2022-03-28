package Assignment3.Indian_mobile_operators;
/**
* This file handles the operations related to reading, setting and returning the url, username, password
* @author Shubham Sharma
*
* **/
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ResourceInitializer {

	public static Logger log = LogManager.getLogger(IndianMobileOperators.class.getName());
	public static Properties configFile = new Properties();
	
	//Initializing properties file
	public static void initializeFile() throws Exception{
		
		try(FileReader reader = new FileReader("src\\main\\resources\\config.properties");){
			
			configFile.load(reader);
		
		}  catch (IOException e) {
			throw new Exception("Config file not loaded/found");
		}
	}
	
	
	//return properties values
	public static String getResource(String property) {
		
		return configFile.getProperty(property);
	}




	
}
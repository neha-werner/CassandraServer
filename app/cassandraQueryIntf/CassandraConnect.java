package cassandraQueryIntf;

import java.io.PrintWriter;
import java.io.StringWriter;

import com.datastax.driver.core.ResultSet;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;



public class CassandraConnect {
	
	private String IP;
	private int port;

	private CassandraConnector cassandraInstanse=null;
	
	public CassandraConnect(String IP,int port){
		
		this.IP = IP;
		this.port = port;
		
		cassandraInstanse=CassandraConnector.getInstance();
	}
	
	public JsonNode connectCassandra(){
		
		//ResultSet resultSet = null;
		
		ArrayNode result = JsonNodeFactory.instance.arrayNode();
		
		try{
			
			cassandraInstanse.connect(IP,port);

		 
			result.add("connected Successfully");	
			
			
		}catch(Exception e){
			
			StringWriter writer = new StringWriter();
			e.printStackTrace(new PrintWriter(writer));

			play.Logger.debug("[Exception Throws In connectCassandra]: "
					+ writer.toString());
			
		}
			
		return result;
				
	}	
}


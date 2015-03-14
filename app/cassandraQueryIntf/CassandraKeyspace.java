package cassandra;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;

import play.libs.Json;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;


public class CassandraKeyspace {
	
	private String query;

	private CassandraConnecter cassandraInstanse=null;
	
	public CassandraKeyspace(String query){
		
		this.query = query;
		
		cassandraInstanse=CassandraConnecter.getInstance();
	}
	
	public JsonNode createSchema(){
		
		ResultSet resultSet = null;
		
		ArrayNode result = JsonNodeFactory.instance.arrayNode();
		
		try{
			
			resultSet = cassandraInstanse.getSession().execute(query);
			result.add("Added New Keyspace");	
			
			
		}catch(Exception e){
			
			StringWriter writer = new StringWriter();
			e.printStackTrace(new PrintWriter(writer));

			play.Logger.debug("[Exception Throws In CassandraKeyspace createSchema]: "
					+ writer.toString());
			
		}
			
		return result;
				
	}	
public JsonNode deleteSchema(){
		
		ResultSet resultSet = null;
		
		ArrayNode result = JsonNodeFactory.instance.arrayNode();
		
		try{
			
			//resultSet = cassandraInstanse.getSession().execute(query);
			//result.add("Deleted Keyspace");	
			resultSet = cassandraInstanse.getSession().execute(query);
			result = resultSetJson(resultSet);
			
		}catch(Exception e){
			
			StringWriter writer = new StringWriter();
			e.printStackTrace(new PrintWriter(writer));

			play.Logger.debug("[Exception Throws In CassandraKeyspace DeleteSchema]: "
					+ writer.toString());
			
		}
			
		return result;
				
	}	
public ArrayNode resultSetJson(ResultSet resultSet)
{
	ArrayNode result = JsonNodeFactory.instance.arrayNode();
	ColumnDefinitions colDef = resultSet.getColumnDefinitions();
	
	
	Iterator<Definition> definitionIterator = colDef.iterator();
	
	ArrayList<String> colNames = new ArrayList<String>();
	
	ArrayList<String> colDataType = new ArrayList<String>();
	
	while(definitionIterator.hasNext()){
		
		Definition definition = definitionIterator.next();
		
		colNames.add(definition.getName());
		
		colDataType.add(definition.getType().toString());
		
		
	}
	
	Iterator<Row> it=resultSet.iterator();
	
	while(it.hasNext()){
		
		Row row = it.next();
		
		ObjectNode resultNodeInner = Json.newObject();
		
			for(int i = 0; i<colNames.size(); i++){
				
				if(colDataType.get(i).equalsIgnoreCase("varchar")){
											
					resultNodeInner.put(colNames.get(i), row.getString(i));
					
				}else if(colDataType.get(i).equalsIgnoreCase("int")){
					
					resultNodeInner.put(colNames.get(i), row.getInt(i));
				}
				
				
			}
					
	  result.add(resultNodeInner);
	}
	return result;
}
public JsonNode createColumnFamily(){
	
	ResultSet resultSet = null;
	
	ArrayNode result = JsonNodeFactory.instance.arrayNode();
	
	try{
		
		//if(cassandraInstanse.getSession())
		//{
		resultSet = cassandraInstanse.getSession().execute(query);
		result.add("Added New columnFamily");	
		//}
		//else
		//	result.add("Not Connected to any cluster");
		
		
	}catch(Exception e){
		
		StringWriter writer = new StringWriter();
		e.printStackTrace(new PrintWriter(writer));

		play.Logger.debug("[Exception Throws In CassandraKeyspace createColumnFamily]: "
				+ writer.toString());
		
	}
		
	return result;
			
}	
public JsonNode getKeyspaces(){
	
	ResultSet resultSet = null;
	
	ArrayNode result = JsonNodeFactory.instance.arrayNode();

	try{

		resultSet = cassandraInstanse.getSession().execute(query);
		result = resultSetJson(resultSet);

		
	}catch(Exception e){
		
		StringWriter writer = new StringWriter();
		e.printStackTrace(new PrintWriter(writer));

		play.Logger.debug("[Exception Throws In CassandraKeyspace getKeyspaces]: "
				+ writer.toString());
		
	}
		
	return result;
			
}	
public JsonNode getTableSchema(){
	
	ResultSet resultSet = null;
	
	ArrayNode result = JsonNodeFactory.instance.arrayNode();

	try{

		resultSet = cassandraInstanse.getSession().execute(query);
		result = resultSetJson(resultSet);

		
	}catch(Exception e){
		
		StringWriter writer = new StringWriter();
		e.printStackTrace(new PrintWriter(writer));

		play.Logger.debug("[Exception Throws In CassandraKeyspace getTableSchema]: "
				+ writer.toString());
		
	}
		
	return result;
			
}
}


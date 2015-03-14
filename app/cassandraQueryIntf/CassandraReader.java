package cassandraQueryIntf;

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

public class CassandraReader {
	private String query;

	private CassandraConnector cassandraConnector = null;

	public CassandraReader(String query) {

		this.query = query;

		cassandraConnector = CassandraConnector.getInstance();
	}

	public JsonNode getDataFromCassandra() {
		ResultSet resultSet = null;

		ArrayNode result = JsonNodeFactory.instance.arrayNode();

		try {

			//cassandraConnector.connect("127.0.0.1", 9042);

			resultSet = cassandraConnector.getSession().execute(query);

			ColumnDefinitions colDef = resultSet.getColumnDefinitions();

			Iterator<Definition> definitionIterator = colDef.iterator();

			ArrayList<String> colNames = new ArrayList<String>();

			ArrayList<String> colDataType = new ArrayList<String>();

			while (definitionIterator.hasNext()) {

				Definition definition = definitionIterator.next();

				colNames.add(definition.getName());

				colDataType.add(definition.getType().toString());

			}

			Iterator<Row> it = resultSet.iterator();

			while (it.hasNext()) {

				Row row = it.next();

				ObjectNode resultNodeInner = Json.newObject();

				for (int i = 0; i < colNames.size(); i++) {

					if (colDataType.get(i).equalsIgnoreCase("varchar")) {

						resultNodeInner.put(colNames.get(i), row.getString(i));

					} else if (colDataType.get(i).equalsIgnoreCase("int")) {

						resultNodeInner.put(colNames.get(i), row.getInt(i));
					}

				}

				result.add(resultNodeInner);
			}

		} catch (Exception e) {

			play.Logger.debug("[Exception Throws In getDataFromCassandra]: "
					+ e.toString());

		} finally {
			cassandraConnector.close();
		}

		return result;

	}

	public JsonNode executeQuery() {
		
		ResultSet resultSet = null;
		
		ArrayNode result = JsonNodeFactory.instance.arrayNode();
		result.add(query);
		try {
			
			cassandraConnector.connect("127.0.0.1", 9042);

			resultSet = cassandraConnector.getSession().execute(query);
			

			

			
			result.add(resultSet.toString());
			
		} catch (Exception e) {

			play.Logger.debug("[Exception Throws In getDataFromCassandra]: "
					+ e.toString());

		} finally {
			cassandraConnector.close();
		}

		return result;

	}
}

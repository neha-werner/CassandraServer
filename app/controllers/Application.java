package controllers;

import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;

import cassandraQueryIntf.CassandraReader;
import play.libs.F.Function;
import play.libs.F.Function0;
import play.libs.F.Promise;
import play.mvc.BodyParser;
import play.mvc.Controller;
import play.mvc.Result;
import views.html.index;

import cassandraQueryIntf.CassandraConnect;
import cassandraQueryIntf.CassandraKeyspace;

import com.fasterxml.jackson.databind.JsonNode;

public class Application extends Controller {

	public static Result index() {
		 return ok(views.html.index.render("Your new application is ready."));
		//return ok("Got request " + request() + "!");
	}


//TODO:comment on why is this about?
    public static Result checkPreFlight() {
    	  response().setHeader("Access-Control-Allow-Origin", "*");
    	  response().setHeader("Access-Control-Allow-Methods", "POST");
    	  response().setHeader("Access-Control-Allow-Headers", "accept, origin, Content-type, x-json, x-prototype-version, x-requested-with");
    	  return ok();
    	}

	@BodyParser.Of(BodyParser.Json.class)
public static Promise<Result> connectToCassandra() {

    	JsonNode requestData = request().body().asJson();
    	
    	Promise<JsonNode> response;
    	
       
    	final CassandraConnect connectcas = new CassandraConnect(requestData.findPath("hostname").asText(),requestData.findPath("port").asInt());
        
        response = Promise.promise(new Function0<JsonNode>() {
          public JsonNode apply() {
            return connectcas.connectCassandra();
          }
        });

        Promise<Result> result = response.map(new Function<JsonNode, Result>() {
          public Result apply(JsonNode json) {
        	  response().setHeader("Access-Control-Allow-Origin", "*");
            return ok(json);
          }
        });
        return result;

      }


    @BodyParser.Of(BodyParser.Json.class)
    public static Promise<Result> getDataFromCassandra() {

    	JsonNode requestData = request().body().asJson();
    	
    	Promise<JsonNode> response;
    	
       
    	final CassandraReader cassandraread = new CassandraReader(requestData.findPath("query").asText());
        
        response = Promise.promise(new Function0<JsonNode>() {
          public JsonNode apply() {
            return cassandraread.getDataFromCassandra();
          }
        });

        Promise<Result> result = response.map(new Function<JsonNode, Result>() {
          public Result apply(JsonNode json) {
        	  response().setHeader("Access-Control-Allow-Origin", "*");
            return ok(json);
          }
        });
        return result;

      }


@BodyParser.Of(BodyParser.Json.class)
public static Promise<Result> createKeyspace() {

    	JsonNode requestData = request().body().asJson();
    	
    	Promise<JsonNode> response;
    	
       
    	final CassandraKeyspace keyspace = new CassandraKeyspace(requestData.findPath("query").asText());
        
        response = Promise.promise(new Function0<JsonNode>() {
          public JsonNode apply() {
            return keyspace.createSchema();
          }
        });

        Promise<Result> result = response.map(new Function<JsonNode, Result>() {
          public Result apply(JsonNode json) {
        	  response().setHeader("Access-Control-Allow-Origin", "*");
            return ok(json);
          }
        });
        return result;

      }
@BodyParser.Of(BodyParser.Json.class)
public static Promise<Result> deleteKeyspace() {

    	JsonNode requestData = request().body().asJson();
    	
    	Promise<JsonNode> response;
    	
       
    	final CassandraKeyspace keyspace = new CassandraKeyspace(requestData.findPath("query").asText());
        
        response = Promise.promise(new Function0<JsonNode>() {
          public JsonNode apply() {
            return keyspace.deleteSchema();
          }
        });

        Promise<Result> result = response.map(new Function<JsonNode, Result>() {
          public Result apply(JsonNode json) {
            return ok(json);
          }
        });
        return result;

      }


@BodyParser.Of(BodyParser.Json.class)
public static Promise<Result> getKeyspaces() {

    	JsonNode requestData = request().body().asJson();
    	
    	Promise<JsonNode> response;
    	
       
    	final CassandraKeyspace keyspaces = new CassandraKeyspace(requestData.findPath("query").asText());
        
        response = Promise.promise(new Function0<JsonNode>() {
          public JsonNode apply() {
            return keyspaces.getKeyspaces();
          }
        });

        Promise<Result> result = response.map(new Function<JsonNode, Result>() {
          public Result apply(JsonNode json) {
        	 response().setHeader("Access-Control-Allow-Origin", "*");
            return ok(json);
          }
        });
        return result;

      }

	public static Promise<Result> getRows(String keyspace_name, String table_name) {
		//JsonNode requestData = request().body().asJson()
		Promise<JsonNode> response;

		final CassandraReader cassandraread = new CassandraReader("SELECT * FROM " + 
				keyspace_name+"."+table_name);

		response = Promise.promise(new Function0<JsonNode>() {
			public JsonNode apply() {
				return cassandraread.getDataFromCassandra();
			}
		});

		Promise<Result> result = response.map(new Function<JsonNode, Result>() {
			public Result apply(JsonNode json) {
				return ok(json);
			}
		});
		return result;

	}

	/*
	 * Add a Row 
	 * 
	 * The POST Json will be of the format: 
	 * {
	 * 	"row":{"columns":["col1","col2","col2"],
	 * 			"values":["val1","val2","val3"] 
	 * 		}
	 * }
	 */
	@BodyParser.Of(BodyParser.Json.class)
	public static Promise<Result> addRow(String keyspace_name, String table_name) {

		JsonNode requestData = request().body().asJson();

		Promise<JsonNode> response;
		/*
		 * Find a way to loop through all the elements in the request data and
		 * prepae a query accordingly
		 */

		StringBuilder query = new StringBuilder("INSERT INTO " + keyspace_name
				+ "." + table_name + " (");

		Iterator<JsonNode> rowDataColumns = requestData.findPath("row")
				.findPath("columns").elements();
		Iterator<JsonNode> rowDataValues = requestData.findPath("row")
				.findPath("values").elements();

		//if (rowDataColumns.size() != rowDataValues.size())
			// return ok("Not a valid insert Query"); //TODO: This is not OK!!

			while (rowDataColumns.hasNext()) {
				query.append(rowDataColumns.next().textValue());
				if (rowDataColumns.hasNext())
					query.append(",");
			}

		query.append(") VALUES (");

		while (rowDataValues.hasNext()) {
			query.append(rowDataValues.next().textValue());
			if (rowDataValues.hasNext())
				query.append(",");
		}
		query.append(")");
		play.Logger.debug("Final Query:" + query.toString());
		final CassandraReader cassandraread = new CassandraReader(
				query.toString());

		response = Promise.promise(new Function0<JsonNode>() {
			public JsonNode apply() {
				return cassandraread.executeQuery();
			}
		});

		Promise<Result> result = response.map(new Function<JsonNode, Result>() {
			public Result apply(JsonNode json) {
				return ok(json);
			}
		});
		return result;

	}
	
	/*
	 * Delete a row
	 * 
	 * The DELETE Json will be of the format: 
	 * {
	 * "row":{"columns":["col1","col2","col2"],
	 * 			"condition":"Acondition" 
	 * 		  }
	 * }
	 */
	@BodyParser.Of(BodyParser.Json.class)
	public static Promise<Result> deleteRow(String keyspace_name, String table_name) {

		JsonNode requestData = request().body().asJson();

		Promise<JsonNode> response;
		/*
		 * Find a way to loop through all the elements in the request data and
		 * prepae a query accordingly
		 */

		StringBuilder query = new StringBuilder("DELETE ");
		

		Iterator<JsonNode> rowDataColumns = requestData.findPath("row")
				.findPath("columns").elements();
		
		while (rowDataColumns.hasNext()) {
			query.append(rowDataColumns.next().textValue());
			if (rowDataColumns.hasNext())
				query.append(",");
		}
		
		query.append(" FROM " + keyspace_name
				+ "." + table_name + " WHERE " + requestData.findPath("row")
				.findPath("condition").textValue() );

		
		
		final CassandraReader cassandraread = new CassandraReader(
				query.toString());

		response = Promise.promise(new Function0<JsonNode>() {
			public JsonNode apply() {
				return cassandraread.executeQuery();
			}
		});

		Promise<Result> result = response.map(new Function<JsonNode, Result>() {
			public Result apply(JsonNode json) {
				return ok(json);
			}
		});
		return result;

	}


	/*
	 * update a Row:
	 * 
	 * The PUT Json will be of the format: 
	 * {
	 * "row":{	 "columns":["col1","col2","col2"],
	 * 			"values":["val1","val2","val3"] ,
	 * 			"condition":"Acondition" 
	 * 		  }
	 * }
	 */
	@BodyParser.Of(BodyParser.Json.class)
	public static Promise<Result> updateRow(String keyspace_name, String table_name) {

		JsonNode requestData = request().body().asJson();

		Promise<JsonNode> response;
		/*
		 * Find a way to loop through all the elements in the request data and
		 * prepae a query accordingly
		 */

		StringBuilder query = new StringBuilder("UPDATE "+ keyspace_name + "." + table_name 
				+" SET ");
		

		Iterator<JsonNode> rowDataColumns = requestData.findPath("row")
				.findPath("columns").elements();
		
		Iterator<JsonNode> rowDataValues = requestData.findPath("row")
				.findPath("values").elements();
		
		while (rowDataColumns.hasNext()) {
			query.append(rowDataColumns.next().textValue() + " = " + rowDataValues.next().textValue());
			if (rowDataColumns.hasNext())
				query.append(",");
		}
		
		query.append(" WHERE " + requestData.findPath("row")
				.findPath("condition").textValue() );

		
		
		final CassandraReader cassandraread = new CassandraReader(
				query.toString());

		response = Promise.promise(new Function0<JsonNode>() {
			public JsonNode apply() {
				return cassandraread.executeQuery();
			}
		});

		Promise<Result> result = response.map(new Function<JsonNode, Result>() {
			public Result apply(JsonNode json) {
				return ok(json);
			}
		});
		return result;

	}
	
	
	/*
	 * create a table:
	 * POST  /keyspace/:keyspace_name/table/	controllers.Application.createTable(keyspace_name: String)
	 * The POST Json will be of the format: 
	 * {
	 * "table":{	 "name":"tableName",
	 * 				"columns":[
	 * 							{"name":"col1",
	 * 							 "type":"coltype1"},
	 * 							{"name":"col2",
	 * 							  "type":"coltype2"}
	 * 						],
	 * 				"primarykeys":["col1","col2"]
	 * 		}
	 * }
	 */
	@BodyParser.Of(BodyParser.Json.class)
	public static Promise<Result> createTable(String keyspace_name) {

		JsonNode requestData = request().body().asJson();

		Promise<JsonNode> response;
		

		StringBuilder query = new StringBuilder("CREATE TABLE "+ 
						keyspace_name + "." + requestData.findPath("name").textValue() 
				+" ( ");
		

		//Add columns and their types
		Iterator<JsonNode> columns = requestData.findPath("columns").elements();
		
		
		while (columns.hasNext()) {
			JsonNode curCol = columns.next();
			query.append(curCol.findPath("name").textValue() + " " + 
					curCol.findPath("type").textValue());
			if (columns.hasNext())
				query.append(",");
		}
		
		//Add primary keys
		Iterator<JsonNode> primaryKeys = requestData.findPath("primarykeys").elements();
		
		if(primaryKeys.hasNext())
		{
			query.append(", PRIMARY KEY  (");
		
		while (primaryKeys.hasNext()) {
			query.append(primaryKeys.next().textValue());
			if (primaryKeys.hasNext())
				query.append(",");
		}
		query.append(" ) " );

		}
		
		query.append(")");
		
		final CassandraReader cassandraread = new CassandraReader(
				query.toString());

		response = Promise.promise(new Function0<JsonNode>() {
			public JsonNode apply() {
				return cassandraread.executeQuery();
			}
		});

		Promise<Result> result = response.map(new Function<JsonNode, Result>() {
			public Result apply(JsonNode json) {
				return ok(json);
			}
		});
		return result;

	}
	
	
	/*
	 * describe the table:
	 * GET  /keyspace/:keyspace_name/table/:table_name	controllers.Application.describeTable(keyspace_name: String, table_name: String)
	 * 
	 */
	public static Promise<Result> describeTable(String keyspace_name, String table_name) {

		
		Promise<JsonNode> response;
		
		StringBuilder query = new StringBuilder("SELECT * FROM system.schema_columns WHERE keyspace_name ='"
				+keyspace_name+"'  AND columnfamily_name = '"+ table_name + "'");
		//StringBuilder query = new StringBuilder("DESCRIBE TABLE "+ 
					//	keyspace_name + "." + table_name);
		

				
		final CassandraReader cassandraread = new CassandraReader(
				query.toString());

		response = Promise.promise(new Function0<JsonNode>() {
			public JsonNode apply() {
				return cassandraread.getDataFromCassandra();
			}
		});

		Promise<Result> result = response.map(new Function<JsonNode, Result>() {
			public Result apply(JsonNode json) {
				return ok(json);
			}
		});
		return result;

	}
	
	/*
	 * delete the table:
	 * DELETE  /keyspace/:keyspace_name/table/:table_name	controllers.Application.deleteTable(keyspace_name: String, table_name: String)
	 */
	public static Promise<Result> deleteTable(String keyspace_name, String table_name) {

	

		Promise<JsonNode> response;
		

		StringBuilder query = new StringBuilder("DROP TABLE "+ 
						keyspace_name + "." + table_name);
		

				
		final CassandraReader cassandraread = new CassandraReader(
				query.toString());

		response = Promise.promise(new Function0<JsonNode>() {
			public JsonNode apply() {
				return cassandraread.executeQuery();
			}
		});

		Promise<Result> result = response.map(new Function<JsonNode, Result>() {
			public Result apply(JsonNode json) {
				return ok(json);
			}
		});
		return result;

	}
	
	
	/*
	 * Add a column:
	 * POST  /keyspace/:keyspace_name/table/:table_name/column	controllers.Application.addColumn(keyspace_name: String, table_name: String)
	 * 
	 * The POST Json will be of the format: 
	 * {
	 * "column":{	 "name":"colName",
	 * 				 "type":"colType"
	 *  		  }
	 * }
	 */
	@BodyParser.Of(BodyParser.Json.class)
	public static Promise<Result> addColumn(String keyspace_name, String table_name) {

		JsonNode requestData = request().body().asJson();

		Promise<JsonNode> response;
		/*
		 * Find a way to loop through all the elements in the request data and
		 * prepae a query accordingly
		 */

		StringBuilder query = new StringBuilder("ALTER TABLE "+ keyspace_name + "." + table_name 
				+" ADD " + requestData.findPath("column").findPath("name").textValue()+ " " + 
				requestData.findPath("column").findPath("type").textValue());
		

		final CassandraReader cassandraread = new CassandraReader(
				query.toString());

		response = Promise.promise(new Function0<JsonNode>() {
			public JsonNode apply() {
				return cassandraread.executeQuery();
			}
		});

		Promise<Result> result = response.map(new Function<JsonNode, Result>() {
			public Result apply(JsonNode json) {
				return ok(json);
			}
		});
		return result;

	}
	
	/*
	 * Alter a column: only type change
	 * PUT  /keyspace/:keyspace_name/table/:table_name/column/:column_name	controllers.Application.AlterColumn(keyspace_name: String, table_name: String, column_name: String)
	 * 
	 * 
	 * The PUT Json will be of the format: 
	 * {
	 * 				 "type":"colType"
	 * }
	 */
	@BodyParser.Of(BodyParser.Json.class)
	public static Promise<Result> AlterColumn(String keyspace_name, String table_name, String column_name) {

		JsonNode requestData = request().body().asJson();

		Promise<JsonNode> response;
		
		StringBuilder query = new StringBuilder("ALTER TABLE "+ keyspace_name + "." + table_name 
				+" ALTER " + column_name + " TYPE " + 
				requestData.findPath("type").textValue());
		

		final CassandraReader cassandraread = new CassandraReader(
				query.toString());

		response = Promise.promise(new Function0<JsonNode>() {
			public JsonNode apply() {
				return cassandraread.executeQuery();
			}
		});

		Promise<Result> result = response.map(new Function<JsonNode, Result>() {
			public Result apply(JsonNode json) {
				return ok(json);
			}
		});
		return result;

	}
	
	
	/*
	 * delete a column:
	 * DELETE  /keyspace/:keyspace_name/table/:table_name/column/:column_name	controllers.Application.deleteColumn(keyspace_name: String, table_name: String, column_name: String)
	 */
	
	public static Promise<Result> deleteColumn(String keyspace_name, String table_name, String column_name) {

		

		Promise<JsonNode> response;
		
		StringBuilder query = new StringBuilder("ALTER TABLE "+ keyspace_name + "." + table_name 
				+" DROP " + column_name );
		

		final CassandraReader cassandraread = new CassandraReader(
				query.toString());

		response = Promise.promise(new Function0<JsonNode>() {
			public JsonNode apply() {
				return cassandraread.executeQuery();
			}
		});

		Promise<Result> result = response.map(new Function<JsonNode, Result>() {
			public Result apply(JsonNode json) {
				return ok(json);
			}
		});
		return result;

	}
	
}

package controllers;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.*;

import play.mvc.*;
import play.test.*;
import play.data.DynamicForm;
import play.data.validation.ValidationError;
import play.data.validation.Constraints.RequiredValidator;
import play.i18n.Lang;
import play.libs.F;
import play.libs.F.*;
import play.libs.Json;

import static play.test.Helpers.*;
import static org.fest.assertions.Assertions.*;

import static org.fest.assertions.Assertions.assertThat;
import static play.mvc.Http.Status.OK;
import static play.test.Helpers.GET;
import static play.test.Helpers.callAction;
import static play.test.Helpers.charset;
import static play.test.Helpers.contentAsString;
import static play.test.Helpers.contentType;
import static play.test.Helpers.status;
import controllers.Application;

import org.junit.Test;

import play.mvc.Result;
import play.test.FakeRequest;

import com.fasterxml.jackson.databind.JsonNode;


/**
*
* Simple (JUnit) tests that can call all parts of a play app.
* If you are interested in mocking a whole application, see the wiki for more details.
*
*/
public class ApplicationTest {

    
        @Test
        public void testConnection(){
        	FakeApplication fakeApplication=fakeApplication();
        	start(fakeApplication);
        	String body = "{\"hostname\":\"127.0.0.1\", \"port\": \"9042\"}";
            JsonNode json = Json.parse(body);
            FakeRequest request = new FakeRequest(POST, "/connection").withJsonBody(json);
            Result result = callAction(controllers.routes.ref.Application.connectToCassandra(), request);
            assertThat(status(result)).isEqualTo(CREATED);
            assertThat(contentType(result)).isEqualTo("application/json");
            assertThat(contentAsString(result)).contains("connected Successfully");
            stop(fakeApplication);
        }
        
        // Unit test create keyspace
        @Test
        public void testGetKeyspaces(){
        	FakeApplication fakeApplication=fakeApplication();
        	start(fakeApplication);
            FakeRequest request = new FakeRequest(GET, "/keyspace");
            Result result = callAction(controllers.routes.ref.Application.getKeyspaces(), request);
            assertThat(status(result)).isEqualTo(OK);
            assertThat(contentType(result)).isEqualTo("application/json");
            assertThat(contentAsString(result)).contains("system_traces");
            //assertThat(contentAsString(result)).contains("already exists");
            stop(fakeApplication);
        }
        // Unit test create keyspace
        @Test
        public void testCreateKeyspace(){
        	FakeApplication fakeApplication=fakeApplication();
        	start(fakeApplication);
        	String body = "{\"keyspacename\":\"UnitTest\", \"kclass\":\"SimpleStrategy\", \"replication_factor\":3}";
            JsonNode json = Json.parse(body);
            FakeRequest request = new FakeRequest(POST, "/keyspace").withJsonBody(json);
            Result result = callAction(controllers.routes.ref.Application.createKeyspace(), request);
            assertThat(status(result)).isEqualTo(CREATED);
            assertThat(contentType(result)).isEqualTo("application/json");
            assertThat(contentAsString(result)).contains("Added New Keyspace");
            //assertThat(contentAsString(result)).contains("already exists");
            stop(fakeApplication);
        }
        
        @Test
        public void testDropKeyspace(){
        	FakeApplication fakeApplication=fakeApplication();
        	start(fakeApplication);
        	String nameKeyspace ="UnitTest";
            FakeRequest request = new FakeRequest(DELETE, "/keyspace/"+nameKeyspace);
            Result result = callAction(controllers.routes.ref.Application.deleteKeyspace(nameKeyspace), request);
            assertThat(status(result)).isEqualTo(OK);
            assertThat(contentType(result)).isEqualTo("application/json");
            assertThat(contentAsString(result)).contains("");
            stop(fakeApplication);
        }
        
       
        
        //Unit Test Keyspace Schema
        @Test
        public void testKeyspaceSchema(){
        	FakeApplication fakeApplication=fakeApplication();
        	start(fakeApplication);
        	//String body = "{\"keyspacename\":\"UnitTest\", \"kclass\":\"SimpleStrategy\", \"replication_factor\":3}";
            //JsonNode json = Json.parse(body);
            FakeRequest request = new FakeRequest(GET, "/keyspace/demo/table");
            Result result = callAction(controllers.routes.ref.Application.getTableSchema("demo"), request);
            assertThat(status(result)).isEqualTo(OK);
            assertThat(contentType(result)).isEqualTo("application/json");
            assertThat(contentAsString(result)).contains("{\"columnfamily_name\":\"books\"}");
            stop(fakeApplication);
        }
        
        //Unit Test create table
        @Test
        public void testCreateTable(){
        	FakeApplication fakeApplication=fakeApplication();
        	start(fakeApplication);
        	String body = "{\"table\":{	\"name\":\"UnitTest\",\"columns\":[{\"name\":\"Title\",\"type\":\"text\"},{\"name\":\"ISBN\",\"type\":\"text\"},{\"name\":\"Author\",\"type\":\"Text\"}],\"primarykeys\":[\"ISBN\"]}}";
            JsonNode json = Json.parse(body);
            FakeRequest request = new FakeRequest(POST, "/keyspace/demo/table").withJsonBody(json);
            Result result = callAction(controllers.routes.ref.Application.createTable("demo"), request);
            assertThat(status(result)).isEqualTo(CREATED);
            assertThat(contentType(result)).isEqualTo("application/json");
            assertThat(contentAsString(result)).contains("Success");
            stop(fakeApplication);
        }
        
        //Unit Test Table Metadata
        @Test
        public void testTableMetaData(){
        	FakeApplication fakeApplication=fakeApplication();
        	start(fakeApplication);
            FakeRequest request = new FakeRequest(GET, "/keyspace/demo/table/books");
            Result result = callAction(controllers.routes.ref.Application.describeTable("demo","books"), request);
            assertThat(status(result)).isEqualTo(OK);
            assertThat(contentType(result)).isEqualTo("application/json");
            assertThat(contentAsString(result)).contains("\"column_name\":\"author\"");
            stop(fakeApplication);
        }
        
        @Test
        public void testTableData(){
        	FakeApplication fakeApplication=fakeApplication();
        	start(fakeApplication);
            FakeRequest request = new FakeRequest(GET, "/keyspace/demo/table/books/row");
            Result result = callAction(controllers.routes.ref.Application.getRows("demo","books"), request);
            assertThat(status(result)).isEqualTo(OK);
            assertThat(contentType(result)).isEqualTo("application/json");
            assertThat(contentAsString(result)).contains("\"author\":");
            stop(fakeApplication);
        }
        
        
        
        //Unit Test Drop table
        @Test
        public void testDropTable(){
        	FakeApplication fakeApplication=fakeApplication();
        	start(fakeApplication);
            FakeRequest request = new FakeRequest(DELETE, "/keyspace/demo/table/UnitTest");
            Result result = callAction(controllers.routes.ref.Application.deleteTable("demo","UnitTest"), request);
            assertThat(status(result)).isEqualTo(OK);
            assertThat(contentType(result)).isEqualTo("application/json");
            assertThat(contentAsString(result)).contains("Success");
            stop(fakeApplication);
        }
       
        
       


        // Disconnect
       /* @Test
        public void closeConnection(){
        	FakeApplication fakeApplication=fakeApplication();
        	start(fakeApplication);
            FakeRequest request = new FakeRequest(DELETE, "/connection");
            Result result = callAction(controllers.routes.ref.Application.closeCassandra(), request);
            assertThat(status(result)).isEqualTo(OK);
            assertThat(contentType(result)).isEqualTo("application/json");
            assertThat(contentAsString(result)).contains("disconnected Successfully");
            stop(fakeApplication);
        }*/

}

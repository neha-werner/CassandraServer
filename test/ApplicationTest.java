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
    public void simpleCheck() {
        int a = 1 + 1;
        assertThat(a).isEqualTo(2);
    }

    @Test
    public void renderTemplate() {
        Content html = views.html.index.render("Your new application is ready.");
        assertThat(contentType(html)).isEqualTo("text/html");
        assertThat(contentAsString(html)).contains("Your new application is ready.");
    }
    
        @Test
        public void testConnection(){
        	FakeApplication fakeApplication=fakeApplication();
        	start(fakeApplication);
        	String body = "{\"hostname\":\"127.0.0.1\", \"port\": \"9042\"}";
            JsonNode json = Json.parse(body);
            FakeRequest request = new FakeRequest(POST, "/connection").withJsonBody(json);
            Result result = callAction(controllers.routes.ref.Application.connectToCassandra(), request);
            assertThat(status(result)).isEqualTo(OK);
            assertThat(contentType(result)).isEqualTo("application/json");
            assertThat(contentAsString(result)).contains("connected Successfully");
            stop(fakeApplication);
        }
        
        @Test
        public void testDropKeyspace(){
        	FakeApplication fakeApplication=fakeApplication();
        	start(fakeApplication);
        	String nameKeyspace ="UnitTest";
        	String body = "{\"keyspacename\":\"UnitTest\", \"kclass\":\"SimpleStrategy\", \"replication_factor\":3}";
            JsonNode json = Json.parse(body);
            FakeRequest request = new FakeRequest(DELETE, "/keyspace/"+nameKeyspace);
            Result result = callAction(controllers.routes.ref.Application.deleteKeyspace(nameKeyspace), request);
            assertThat(status(result)).isEqualTo(OK);
            assertThat(contentType(result)).isEqualTo("application/json");
            assertThat(contentAsString(result)).contains("");
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
            assertThat(status(result)).isEqualTo(OK);
            assertThat(contentType(result)).isEqualTo("application/json");
            assertThat(contentAsString(result)).contains("Added New Keyspace");
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
            assertThat(contentAsString(result)).contains("{\"columnfamily_name\":\"users\"}");
            stop(fakeApplication);
        }





}

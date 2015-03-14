package cassandraQueryIntf;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;


public class CassandraConnector {
	private static CassandraConnector CassandraconnectorInstance = null;
	
	/*Cassandra clustor*/
	private Cluster cluster;
	
	private Session session;

	/*
	 * Get the cassandra connector instance*/
	public static CassandraConnector getInstance() {

		if (CassandraconnectorInstance == null) {
			synchronized (CassandraConnector.class) {
				if (CassandraconnectorInstance == null) {
					CassandraconnectorInstance  = new CassandraConnector();
				}
			}

		}
		return CassandraconnectorInstance;
	}
	
	
	   /**
	    * Connect to Cassandra Cluster specified by provided node IP
	    * address and port number.
	    *
	    * @param node Cluster node IP address.
	    * @param port Port of cluster host.
	    */
	   public void connect(final String node, final int port)
	   {
	      cluster = Cluster.builder().addContactPoint(node).withPort(port).build();
	      final Metadata metadata = cluster.getMetadata();
	      play.Logger.info("Connected to cluster:"+metadata.getClusterName());
	
	      session = cluster.connect();
	   }
	   
	   /**
	    * Provide my Session.
	    *
	    * @return My session.
	    */
	   public Session getSession()
	   {
	      return session;
	   }

	   /** Close cluster. */
	   public void close()
	   {
	      cluster.close();
	   }
}

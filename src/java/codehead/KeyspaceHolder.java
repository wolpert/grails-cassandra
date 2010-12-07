package codehead;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

public class KeyspaceHolder extends ThreadLocal <Keyspace>{

	private String clusterName="Test Cluster";
	private String keyspaceName="Keyspace1";
	private String servers="localhost:9160";
		
	protected Keyspace initialValue() {
		return HFactory.createKeyspace(keyspaceName, getCluster());
	}

	/**
	 * Returns the hector cluster object
	 * @return
	 */
	public Cluster getCluster(){
		return HFactory.getOrCreateCluster(clusterName, servers);
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public String getKeyspaceName() {
		return keyspaceName;
	}

	public void setKeyspaceName(String keyspaceName) {
		this.keyspaceName = keyspaceName;
	}

	public String getServers() {
		return servers;
	}

	public void setServers(String servers) {
		this.servers = servers;
	}

	
}

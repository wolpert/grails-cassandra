package codehead;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;

import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;

class CassandraService {
	
	boolean transactional = true
	
	String servers="localhost:9160"
	String clusterName="Test Cluster"
	String keyspaceName="Keyspace1"
	def hideNotFoundExceptions=true
	def convertOnGetDefault = true
	def keyspaceDetails = [:]
	
	
	/**
	 *   The service itself is a singleton, but if we throw it away and create a new one, then we want to do the
	 *   same thing for the keyspace holder. Note the the keyspace hold is not an 
	 *   inner class because groovy 1.6 doesn't support inner classes
	 */
	private final KeyspaceHolder keyspaceHolder= new KeyspaceHolder();

	// need to override the setters so the keyspaceHolder is set right	

	public void setServers(String s){
		log.debug("[setServers(${s})]")
		servers = s;
		keyspaceHolder.setServers(s);
	}
	public void setClusterName(String s){
		log.debug("[setClusterName(${s})]")
		clusterName = s;
		keyspaceHolder.setClusterName(s);
	}
	public void setKeyspaceName(String s){
		log.debug("[setKeyspaceName(${s})]")
		keyspaceName = s;
		keyspaceHolder.setKeyspaceName(s);
	}
	
	/**
	 * Returns the hector cluster object
	 * @return
	 */
	public Cluster getCluster(){
		log.debug("[getCluster()]")
		return keyspaceHolder.getCluster();
	}
	
	public Keyspace keyspace() {
		log.debug("[keyspace()]")
		return keyspaceHolder.get();
	}
	
	def resetKeyspace(){
		log.debug("[resetKeyspace()]")
		keyspaceHolder.remove()
	}
	
	/**
	 * A way get the details of the column families in this keyspace. Note that the data is also stored in
	 * the keyspaceDetails properties, so if you need to flush the cache, set that to null 
	 * @return
	 */
	def columnFamilyDetails(keyspaceName=keyspace){
		if (null==keyspaceDetails[keyspaceName]) {
			keyspaceDetails[keyspaceName] = getCluster().describeKeyspace(keyspaceName)
		}
		return keyspaceDetails[keyspaceName]
	}
	
	/**
	 * Executes the closure giving it the keyspace as an argument
	 */
	def execute(block){
		log.debug("[execute]")
		return block(getKeyspace())
	}
	
	/**
	 * Returns the serializer for the object given...
	 * 
	 * @param object
	 * @return
	 */
	def serializer(object){
		return SerializerTypeInferer.getSerializer(object)
	}
	
	/**
	 * Calls the closure, hiding any NotFoundExceptions only if hideNotFoundExceptions=true
	 */
	def exceptionCatcher(block){
		try {
			return block()
		} catch (NotFoundException nfe){
			if (hideNotFoundExceptions){
				return null;
			} else {
				throw(nfe)
			}
		}
	}
	
	/**
	 * Sets a single value for a key. Note that it uses the potential serializer derived from the data type.
	 * 
	 * @param key
	 * @param cf
	 * @param scName Optional super column name... needs to be set if and only if its a super column family
	 * @param name
	 * @param value
	 * @return
	 */
	def setValue(key,cf,scName=null,name,value){
		Mutator m = HFactory.createMutator(keyspace(),serializer(key));
		MutationResult mr = m.insert(key, cf, HFactory.createColumn(name, value, serializer(name), serializer(value)));
	}

	/**
	* Gets a single value for a key. Note that it uses the potential serializer derived from the data type.
	*
	* @param key
	* @param cf
	* @param scName Optional super column name... needs to be set if and only if its a super column family
	* @param name
	* @param valueClass, expected type coming back.
	* @return
	*/
   def getValue(key,cf,scName=null,name,valueClass){
		ColumnQuery q = HFactory.createColumnQuery(keyspace(), serializer(key), serializer(name), serializer(valueClass))
		q.setName(name).setColumnFamily(cf)
		QueryResult r = q.setKey(key).execute()
		HColumn c = r.get()
		if(c==null){
			return null
		} else {
			return c.getValue()
		}
   }
   
   /**
    * Removes the column for the key's name from the (super) column family
    * @param key
    * @param cf
	* @param scName Optional super column name... needs to be set if and only if its a super column family
    * @param name
    * @return
    */
   def deleteValue(key,cf,scName=null,name){
	   Mutator m = HFactory.createMutator(keyspace(), serializer(key));
	   MutationResult mr2 = m.delete(key, cf, name, serializer(name));
   }

}

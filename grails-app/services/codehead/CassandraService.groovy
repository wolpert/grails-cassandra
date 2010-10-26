package codehead;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;

import me.prettyprint.cassandra.serializers.BytesSerializer;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;

class MutatorHandler {
	
	def keyspace
	def serializer
	def hMutator
	
	def MutatorHandler(keyspace, serializer){
		this.keyspace = keyspace
		this.serializer = serializer
		this.hMutator = HFactory.createMutator(keyspace, serializer)
	}
	
	def insertSuperColumn(key,cf,scName,map){
		def list = []
		def nameSerializer, valueSerializer 
		map.each{k,v-> list << HFactory.createColumn(k,v,(nameSerializer=serializer(k)), (valueSerializer=serializer(v)))}
		def superColumn = HFactory.createSuperColumn(scName,list,serializer(scName),nameSerializer, valueSerializer)
		hMutator.insert(key,cf,superColumn)
	}
	// TODO: Test scName != null
	def insert(key,cf,scName=null,name,value){
		def col = HFactory.createColumn(name,value, serializer(name), serializer(value))
		if(scName!=null){
			col = HFactory.createSuperColumn(scName,Arrays.asList(col),serializer(scName), serializer(name), serializer(value))
		}
		hMutator.addInsertion(key, cf, col);
	}
	// TODO: Test scName != null
	def delete(key,cf,scName=null,name){
		if(scName==null){
			hMutator.addDeletion(key, cf, name, serializer(name));
		} else {
			hMutator.subDelete(key, cf, scName, name,serializer(scName), serializer(name));
		}
	}
	def serializer(object){
		if(object==null){
			object=byte[].class
		}
		return SerializerTypeInferer.getSerializer(object)
	}


}

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
		if(object==null){
			object=byte[].class
		}
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
	 * Will provide a way to add inserts in a batch way... give it a closure and it will give you a 'pseudo-atomic' mutator.
	 * @param cf
	 * @param keyClass
	 * @return
	 */
	def batchKeyUpdate(keyClass,block){
		MutatorHandler mutatorHandler = new MutatorHandler(keyspace(), serializer(keyClass));
		block.delegate = mutatorHandler
		block()
		mutatorHandler.hMutator.execute()
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
		def q 
		if (scName==null){
			q = HFactory.createColumnQuery(keyspace(), serializer(key), serializer(name), serializer(valueClass))
			q.setName(name)
		} else {
			q = HFactory.createSubColumnQuery(keyspace(), serializer(key), serializer(scName), serializer(name), serializer(valueClass))
			q.setSuperColumn(scName).setColumn(name)
		}
		q.setColumnFamily(cf).setKey(key)
		QueryResult r = q.execute()
		HColumn c = r.get()
		if(c==null){
			return null
		} else {
			return c.getValue()
		}
   }
   
   /**
    * Returns a map of all values for the column and/or super column
    * TODO: Test NOT having a super column
    * @param key
    * @param cf
    * @param nameClass
    * @param valueClass
    * @return
    */
   def getValues(key,cf,scName=null,nameClass,valueClass){
	   def q
	   q = HFactory.createSuperColumnQuery(keyspace(), serializer(key), serializer(scName), serializer(nameClass), serializer(valueClass))
	   q.setSuperName(scName).setColumnFamily(cf)
	   q.setKey(key)
	   
	   def qr = q.execute()
	   // deal with the super column
	   def sc = qr.get()
	   def map=[:]
	   if(sc){
		   sc.getColumns().each{map[it.getName()]=it.getValue()}
	   }
	   return map
   }
   
   /**
    * TODO: Test using a super column
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

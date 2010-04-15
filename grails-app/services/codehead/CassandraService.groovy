package codehead;

import java.util.ArrayList;

import me.prettyprint.cassandra.service.CassandraClientPool
import me.prettyprint.cassandra.service.CassandraClientPoolFactory
import me.prettyprint.cassandra.service.CassandraClient
import me.prettyprint.cassandra.service.Keyspace

import static me.prettyprint.cassandra.utils.StringUtils.bytes;
import static me.prettyprint.cassandra.utils.StringUtils.string;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.ColumnParent
import org.apache.cassandra.thrift.SliceRange
import org.apache.cassandra.thrift.SliceRange
import org.apache.cassandra.thrift.SlicePredicate
import org.apache.cassandra.thrift.ConsistencyLevel

class CassandraService {
	
	boolean transactional = true
	
	def servers=["localhost:9160"]
	def defaultKeyspace="Keyspace1"
	def hideNotFoundExceptions=true
	
	/**
	 * Executes the block passing in the available client
	 */
	def acquireClient(block){
		//if(null!=log) log.debug("[acquireClient]")
		CassandraClientPool pool = CassandraClientPoolFactory.INSTANCE.get();
		CassandraClient client = pool.borrowClient(servers);
		try {
			return block(client)
		} finally {
			pool.releaseClient(client)
		}
	}
	
	/**
	 * Executes the closure giving it the keyspace as an argument
	 */
	def execute(keyspaceName=defaultKeyspace,block){
		//if(null!=log) log.debug("[execute] $keyspaceName")
		acquireClient {  client ->
			Keyspace keyspace = client.getKeyspace(keyspaceName);
			return block(keyspace)
		}
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
	 * Returns the number of columns defined for this key in this column family. If the key is not defined, will return a zero and
	 * not throw an exception.
	 * 
	 * @param columnFamilyName
	 * @param key
	 * @param superColumnName (optional)
	 * @return number of columns available.
	 */
	def getColumnCount(columnFamilyName,key,superColumnName=null){
		def columnParent = new ColumnParent(columnFamilyName, (superColumnName==null ? null : bytes(superColumnName)))
		execute { keyspace ->
			def result = exceptionCatcher{keyspace.getCount(key,columnParent)}
			if(result==null){
				result=0
			}
			return result
		}
		
	}
	
	
	/**
	 * This will return a single value for a specific column family.
	 * superColumnName==null
	 *   columnFamilyName:{key:{columnName = columnValue}}
	 * superColumnName!=null
	 *   columnFamilyName:{key:{superColumnName:{columnName = columnValue}}}
	 *   
	 * @param columnFamilyName
	 * @param key
	 * @param superColumnName (optional)
	 * @param columnName
	 * @return
	 */
	def getColumnValue(columnFamilyName,key,superColumnName=null,columnName){
		//if(null!=log) log.debug("[getColumnValue] $columnFamilyName $key $superColumnName $columnName")
		def columnPath = getColumnPath(columnFamilyName,superColumnName,columnName)
		execute { keyspace ->
			return exceptionCatcher{
				string(keyspace.getColumn(key,columnPath).getValue())
			}
		}
	}
	
	/**
	 * This will set the value for a specific column family.
	 * superColumnName==null
	 *   columnFamilyName:{key:{columnName = columnValue}}
	 * superColumnName!=null
	 *   columnFamilyName:{key:{superColumnName:{columnName = columnValue}}}
	 * 
	 * @param columnFamilyName
	 * @param key
	 * @param superColumnName (optional)
	 * @param columnName
	 * @param value
	 * @return
	 */
	def setColumnValue(columnFamilyName,key,superColumnName=null,columnName,value){
		//if(null!=log) log.debug("[setColumnValue] $columnFamilyName $key $superColumnName $columnName $value")
		def columnPath = getColumnPath(columnFamilyName,superColumnName,columnName)
		execute { keyspace ->
			def old_value =  null
			try {old_value = string(keyspace.getColumn(key,columnPath).getValue())
			} catch (NotFoundException nfe){;
			}
			keyspace.insert(key, columnPath, bytes(value))
			return old_value
		}
	}
	
	/**
	 * Sets all the column name/value pairs in the values map 
	 * 
	 * @param columnFamilyName
	 * @param key
	 * @param superColumnName (optional)
	 * @param values map of columnNames to columnValues.
	 * @return
	 */
	def setColumnValues(columnFamilyName,key, superColumnName=null,values){
		//if(null!=log) log.debug("[setColumnValues]  $columnFamilyName $key $superColumnName $values")
		ArrayList<Column> list = new ArrayList<Column>(values.size()); // columnName,value in values
		values.each{
			list.add(new Column(bytes(it.key.toString()),bytes(it.value.toString()),System.currentTimeMillis()));
		}
		// if no superColumnName
		if (null==superColumnName){
			HashMap<String, List<Column>> cfmap = new HashMap<String, List<Column>>(values.size());
			cfmap.put(columnFamilyName,list)
			execute {keyspace -> keyspace.batchInsert(key,cfmap,null)}
		} else {
			SuperColumn sc = new SuperColumn(bytes(superColumnName),list)
			HashMap<String, List<SuperColumn>> cfmap = new HashMap<String, List<SuperColumn>>(1);
			List<SuperColumn> scList = new ArrayList<SuperColumn>(1)
			scList.add(sc)
			cfmap.put(columnFamilyName,scList)
			execute {keyspace -> keyspace.batchInsert(key,null,cfmap)}
		}
	}
	
	
	/**
	 * //supercolumnname/columnfamilyname/key?colname=colval&colname=colval
	 */
	def getColumnPath(columnFamilyName,superColumnName=null,name){
		//if(null!=log) log.debug("[getColumnPath]: $columnFamilyName $superColumnName $name")
		ColumnPath cp = new ColumnPath(columnFamilyName)
		cp.setColumn(bytes(name))
		if(superColumnName!=null){
			cp.setSuper_column(bytes(superColumnName))
		}
		return cp
	}
	
	
	
}

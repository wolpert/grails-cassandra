package codehead;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.UUID;

import me.prettyprint.cassandra.service.CassandraClientPool
import me.prettyprint.cassandra.service.CassandraClientPoolFactory
import me.prettyprint.cassandra.service.CassandraClient
import me.prettyprint.cassandra.service.Keyspace

import static me.prettyprint.cassandra.utils.StringUtils.bytes;
import static me.prettyprint.cassandra.utils.StringUtils.string;

import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.ColumnParent
import org.apache.cassandra.thrift.SliceRange
import org.apache.cassandra.thrift.SlicePredicate
import org.apache.cassandra.thrift.ConsistencyLevel

class CassandraService {
	
	boolean transactional = true
	
	def servers=["localhost:9160"]
	def defaultKeyspace="Keyspace1"
	def hideNotFoundExceptions=true
	def convertOnGetDefault = true
	def keyspaceDetails = [:]
	                       
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
	 * A way get the details of the column families in this keyspace. Note that the data is also stored in
	 * the keyspaceDetails properties, so if you need to flush the cache, set that to null 
	 * @return
	 */
	def columnFamilyDetails(keyspaceName=defaultKeyspace){
		if (null==keyspaceDetails[keyspaceName]) {
			keyspaceDetails[keyspaceName] = acquireClient{it.getCassandra().describe_keyspace(keyspaceName)}
		}
		return keyspaceDetails[keyspaceName]
	}
	
	/**
	 * Returns a list of keys for the column family. They will not be ordered
	 * if you are not using an OPP sort. You can page through the list by 
	 * starting with emptystrings for startKey and endKey, and using the last
	 * key found as the 'startKey' in the next list.
	 * @param columnFamily
	 * @return
	 */
	def keySearch(columnFamily,count=100,startKey="",endKey=""){
		execute { keyspace ->
			ColumnParent cp = new ColumnParent(columnFamily)
			SliceRange sr = new SliceRange(new byte[0], new byte[0], true, 0) //we don't want any columns, just keys 
			SlicePredicate sp = new SlicePredicate()
			sp.setSlice_range(sr)
			KeyRange kr = new KeyRange()
			kr.setCount(count)
			kr.setStart_key(startKey)
			kr.setEnd_key(endKey)
			def map = keyspace.getRangeSlices(cp,sp,kr)
			return map.keySet()
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
	 * If the object is not already a byte[], it will convert it to one.
	 * @param obj
	 * @return
	 */
	def bytesConvert(obj){
		if (obj instanceof Long){
			obj = ByteBuffer.allocate(8).putLong(obj).array()
		} else if(!(obj instanceof byte[])){
			obj = bytes(obj.toString())
		}
		obj
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
		def columnParent = new ColumnParent(columnFamilyName, (superColumnName==null ? null : bytesConvert(superColumnName)))
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
	 * Removes an entry from the system. 
	 * @param columnFamilyName Required to specify the columnFamily to remove from
	 * @param key Require to specify the key to remove.
	 * @param superColumnName optional if there is a superColumn 
	 * @param columnName optional. Use this if you only want to remove one value
	 */
	def removeValue(columnFamilyName, key, superColumnName=null, columnName=null){
		def columnPath = getColumnPath(columnFamilyName,superColumnName,columnName)
		execute {keyspace ->
			keyspace.remove(key,columnPath)
		}
	}
	
	/**
	 * Returns a map where the key is the super-column, and the value is the sub-column name/value pairs
	 * @param columnFamilyName
	 * @param key
	 * @param covert Set this to false if you want 'bytes' back.
	 * @return
	 */
	def getRow(columnFamilyName,key,convert=convertOnGetDefault){
		def rowDetails = columnFamilyDetails()[columnFamilyName]
		
		SliceRange sr = new SliceRange(new byte[0], new byte[0], false, 1000000); //TODO, fix this to get a real count, somehow
        SlicePredicate predicate = new SlicePredicate();
        predicate.setSlice_range(sr)

		acquireClient { client ->
			List<ColumnOrSuperColumn> columns = client.getCassandra().get_slice(defaultKeyspace, key, 
					new ColumnParent(columnFamilyName),
					predicate, 
					ConsistencyLevel.ONE);
			int size = columns.size();
			def result = new HashMap()
	        for (ColumnOrSuperColumn cosc : columns) {
	            if (cosc.isSetSuper_column()) {
	                SuperColumn superColumn = cosc.super_column;
	                def superColumnName = (convert ? convertValue(superColumn.name,rowDetails["CompareWith"]) :  superColumn.name)
	                result[superColumnName] = new HashMap()
	                for (Column col : superColumn.getColumns()){
	                	result[superColumnName][( convert ? convertValue(col.name,rowDetails["CompareSubcolumnsWith"]) : col.name)] =  (convert ? convertValue(col.value) : col.value)}
	            } else {
	                Column col = cosc.column;
	                result[(convert ? convertValue(col.name,rowDetails["CompareWith"]) : col.name) ] =  ( convert ? convertValue(col.value) : col.value)
	            }
	        } // for	
	        return result
		}
	}
		
	def convertValue(value,type=null){
		def result
		if(type=="org.apache.cassandra.db.marshal.TimeUUIDType"){
	        long msb = 0;
	        long lsb = 0;
	        for (int i=0; i<8; i++)
	            msb = (msb << 8) | (value[i] & 0xff);
	        for (int i=8; i<16; i++)
	            lsb = (lsb << 8) | (value[i] & 0xff);
	        long mostSigBits = msb;
	        long leastSigBits = lsb;
	        def uuid = new UUID(msb, lsb)
	        result =  uuid.toString()
		} else if (type == "org.apache.cassandra.db.marshal.LongType"){
			result = ByteBuffer.wrap(bytes).getLong()
		} else {
			result = new String(value,"UTF-8")
		}
		return result
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
			keyspace.insert(key, columnPath, bytesConvert(value))
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
			list.add(new Column(bytesConvert(it.key),bytesConvert(it.value),System.currentTimeMillis()));
		}
		// if no superColumnName
		if (null==superColumnName){
			HashMap<String, List<Column>> cfmap = new HashMap<String, List<Column>>(values.size());
			cfmap.put(columnFamilyName,list)
			execute {keyspace -> keyspace.batchInsert(key,cfmap,null)}
		} else {
			SuperColumn sc = new SuperColumn(bytesConvert(superColumnName),list)
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
		if(name!=null){
			cp.setColumn(bytesConvert(name))
		}
		if(superColumnName!=null){
			cp.setSuper_column(bytesConvert(superColumnName))
		}
		return cp
	}
	
	
	
}

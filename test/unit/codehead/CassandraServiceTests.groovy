package codehead;

/**
 * 
 *   This file was taken from the hector code base and re-worked to give an example on
 *   how to use the hector way to do things, and the groovy way.
 * 
 */

import grails.test.*
import me.prettyprint.cassandra.serializers.StringSerializer;

import me.prettyprint.hector.api.query.*
import me.prettyprint.hector.api.beans.*
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.*
import static me.prettyprint.hector.api.factory.HFactory.createColumn;
import static me.prettyprint.hector.api.factory.HFactory.createColumnQuery;
import static me.prettyprint.hector.api.factory.HFactory.createCountQuery;
import static me.prettyprint.hector.api.factory.HFactory.createKeyspace;
import static me.prettyprint.hector.api.factory.HFactory.createMultigetSliceQuery;
import static me.prettyprint.hector.api.factory.HFactory.createMultigetSubSliceQuery;
import static me.prettyprint.hector.api.factory.HFactory.createMultigetSuperSliceQuery;
import static me.prettyprint.hector.api.factory.HFactory.createMutator;
import static me.prettyprint.hector.api.factory.HFactory.createRangeSlicesQuery;
import static me.prettyprint.hector.api.factory.HFactory.createRangeSubSlicesQuery;
import static me.prettyprint.hector.api.factory.HFactory.createRangeSuperSlicesQuery;
import static me.prettyprint.hector.api.factory.HFactory.createSliceQuery;
import static me.prettyprint.hector.api.factory.HFactory.createSubColumnQuery;
import static me.prettyprint.hector.api.factory.HFactory.createSubCountQuery;
import static me.prettyprint.hector.api.factory.HFactory.createSubSliceQuery;
import static me.prettyprint.hector.api.factory.HFactory.createSuperColumn;
import static me.prettyprint.hector.api.factory.HFactory.createSuperColumnQuery;
import static me.prettyprint.hector.api.factory.HFactory.createSuperCountQuery;
import static me.prettyprint.hector.api.factory.HFactory.createSuperSliceQuery;
import static me.prettyprint.hector.api.factory.HFactory.getOrCreateCluster;

import org.apache.cassandra.thrift.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TestCleanupDescriptor {
	
	def cf
	def rowCount
	def columnCount
	def columnsPrefix
	def rowPrefix
	def scCount
	def scPrefix
	
	TestCleanupDescriptor(){
	}
	
	TestCleanupDescriptor(String cf, int rowCount, String rowPrefix, int scCount,  String scPrefix) {
		this.cf = cf;
		this.rowCount = rowCount;
		this.rowPrefix = rowPrefix;
		this.columnCount = scCount;
		this.columnsPrefix = scPrefix;
	}
}


class CassandraServiceTests extends GrailsUnitTestCase {
	
	def cassandraService
	private static final StringSerializer se = new StringSerializer();
	private static final Logger log = LoggerFactory.getLogger(CassandraServiceTests.class);
	
	protected void setUp() {
		super.setUp()
		mockLogging(CassandraService,true)
		cassandraService = new CassandraService()
	}
	
	protected void tearDown() {
		super.tearDown()
	}
	
	void testExceptionBlocker(){
		def result = cassandraService.exceptionCatcher { throw new NotFoundException() }
		assertNull(result) // and if you got not error, its all good.
	}
	
	/**
	 * The standard hector test, before the service refactoring...
	 */
	void testSetGetDeleteOldStyle(){
		
		String cf = "Standard1"
		String method = "testSetGetDeleteOldStyle"
		String key = "${method}_Key"
		String name = "${method}_Name"
		String value = "${method}_Value"
		
		Mutator<String> m = HFactory.createMutator(cassandraService.keyspace(),cassandraService.serializer(String.class));
		MutationResult mr = m.insert(key, cf, HFactory.createColumn(name, value, cassandraService.serializer(String.class), cassandraService.serializer(String.class)));
		
		ColumnQuery<String, String, String> q = HFactory.createColumnQuery(cassandraService.keyspace(), cassandraService.serializer(String.class), cassandraService.serializer(String.class), cassandraService.serializer(String.class))
		q.setName(name).setColumnFamily(cf)
		QueryResult<HColumn<String, String>> r = q.setKey(key).execute();
		assertNotNull(r);
		
		HColumn<String, String> c = r.get();
		assertNotNull(c);
		String foundvalue = c.getValue();
		assertEquals(value, foundvalue);
		String foundname = c.getName();
		assertEquals(name, foundname);
		assertEquals(q, r.getQuery());
		
		m = HFactory.createMutator(cassandraService.keyspace(), cassandraService.serializer(String.class));
		MutationResult mr2 = m.delete(key, cf, name, cassandraService.serializer(String.class));
		
		// get already removed value
		ColumnQuery<String, String, String> q2 = HFactory.createColumnQuery(cassandraService.keyspace(), cassandraService.serializer(String.class), cassandraService.serializer(String.class), cassandraService.serializer(String.class));
		q2.setName(name).setColumnFamily(cf);
		QueryResult<HColumn<String, String>> r2 = q2.setKey(key).execute();
		assertNotNull(r2);
		assertNull("Value should have been deleted", r2.get())
	}
	/**
	 * The standard hector test, before the service refactoring...
	 */
	void testSetGetDeleteGroovyStyle(){
		
		String cf = "Standard1"
		String method = "testSetGetDeleteGroovyStyle"
		String key = "${method}_Key"
		String name = "${method}_Name"
		String value = "${method}_Value"
		
		cassandraService.setValue(key,cf,name,value)
		
		def result = cassandraService.getValue(key,cf,name,String.class)
		assertNotNull(result);
		assertEquals(value,result)
		
		cassandraService.deleteValue(key,cf,name)
		assertNull("Value should have been deleted", cassandraService.getValue(key,cf,name,String.class))
	}
	
	void testBatchInsertGetRemove() {
		String cf = "Standard1";
		
		Mutator<String> m = createMutator(cassandraService.keyspace(), se);
		for (int i = 0; i < 5; i++) {
			m.addInsertion("testInsertGetRemove" + i, cf,
					createColumn("testInsertGetRemove", "testInsertGetRemove_value_" + i, se, se));
		}
		m.execute();
		
		// get value
		ColumnQuery<String, String, String> q = createColumnQuery(cassandraService.keyspace(), se, se, se);
		q.setName("testInsertGetRemove").setColumnFamily(cf);
		for (int i = 0; i < 5; i++) {
			QueryResult<HColumn<String, String>> r = q.setKey("testInsertGetRemove" + i).execute();
			assertNotNull(r);
			HColumn<String, String> c = r.get();
			assertNotNull(c);
			String value = c.getValue();
			assertEquals("testInsertGetRemove_value_" + i, value);
		}
		
		// remove value
		m = createMutator(cassandraService.keyspace(), se);
		for (int i = 0; i < 5; i++) {
			m.addDeletion("testInsertGetRemove" + i, cf, "testInsertGetRemove", se);
		}
		m.execute();
		
		// get already removed value
		ColumnQuery<String, String, String> q2 = createColumnQuery(cassandraService.keyspace(), se, se, se);
		q2.setName("testInsertGetRemove").setColumnFamily(cf);
		for (int i = 0; i < 5; i++) {
			QueryResult<HColumn<String, String>> r = q2.setKey("testInsertGetRemove" + i).execute();
			assertNotNull(r);
			assertNull("Value should have been deleted", r.get());
		}
	}
	
	void testBatchInsertGetRemoveGroovy() {
		String cf = "Standard1";
		
		cassandraService.batchKeyUpdate(String.class){
			(0..4).each{ insert("testInsertGetRemove" + it,cf,"testInsertGetRemove", "testInsertGetRemove_value_" + it) }
		}
		
		// get value
		(0..4).each{ i ->
			assertEquals("testInsertGetRemove_value_" + i, cassandraService.getValue("testInsertGetRemove" + i,cf,"testInsertGetRemove",String.class))
		}
		
		// remove value
		cassandraService.batchKeyUpdate(String.class){
			(0..4).each{ delete("testInsertGetRemove" + it,cf,"testInsertGetRemove") }
		}
		
		// get already removed value
		(0..4).each{ i ->
			assertNull("testInsertGetRemove_value_" + i, cassandraService.getValue("testInsertGetRemove" + i,cf,"testInsertGetRemove",String.class))
		}
	}
	
	
	void testSuperInsertGetRemove() {
		String cf = "Super1";
		
		Mutator<String> m = createMutator(cassandraService.keyspace(), se);
		
		@SuppressWarnings("unchecked")
				// aye, varargs and generics aren't good friends...
				List<HColumn<String, String>> columns = Arrays.asList(createColumn("name1", "value1", se, se),
				createColumn("name2", "value2", se, se));
		m.insert("testSuperInsertGetRemove", cf,
				createSuperColumn("testSuperInsertGetRemove", columns, se, se, se));
		
		// get value
		SuperColumnQuery<String, String, String, String> q = createSuperColumnQuery(cassandraService.keyspace(), se, se, se, se);
		q.setSuperName("testSuperInsertGetRemove").setColumnFamily(cf);
		QueryResult<HSuperColumn<String, String, String>> r = q.setKey("testSuperInsertGetRemove").execute();
		assertNotNull(r);
		HSuperColumn<String, String, String> sc = r.get();
		assertNotNull(sc);
		assertEquals(2, sc.getSize());
		HColumn<String, String> c = sc.get(0);
		String value = c.getValue();
		assertEquals("value1", value);
		String name = c.getName();
		assertEquals("name1", name);
		
		HColumn<String, String> c2 = sc.get(1);
		assertEquals("name2", c2.getName());
		assertEquals("value2", c2.getValue());
		
		// remove value
		m = createMutator(cassandraService.keyspace(), se);
		m.subDelete("testSuperInsertGetRemove", cf, "testSuperInsertGetRemove", null, se, se);
		
		// test after removal
		r = q.execute();
		sc = r.get();
		assertNull(sc);
	}

	void testSuperInsertGetRemoveGroovy() {
		String cf = "Super1";
	
		cassandraService.batchKeyUpdate(String.class){
			insertSuperColumn("testSuperInsertGetRemove", cf,"testSuperInsertGetRemove",["name1":"value1","name2":"value2"])
		}
		
		// get value
		// First, test getting each value
		assertEquals("value1",cassandraService.getValue("testSuperInsertGetRemove",cf,"testSuperInsertGetRemove","name1",String.class))
		assertEquals("value2",cassandraService.getValue("testSuperInsertGetRemove",cf,"testSuperInsertGetRemove","name2",String.class))
		// now test getting the map
		def map = cassandraService.getValues("testSuperInsertGetRemove",cf,"testSuperInsertGetRemove",String.class,String.class)
		assertEquals(2,map.size())
		assertEquals('value1',map["name1"])
		assertEquals('value2',map["name2"])
		
		
		// remove value
		//Mutator<String> m = createMutator(cassandraService.keyspace(), se);
		//m.subDelete("testSuperInsertGetRemove", cf, "testSuperInsertGetRemove", null, se, se);
		cassandraService.batchKeyUpdate(String.class){delete("testSuperInsertGetRemove",cf,"testSuperInsertGetRemove",null)}

		// Check again
		assertEquals(0,cassandraService.getValues("testSuperInsertGetRemove",cf,"testSuperInsertGetRemove",String.class,String.class).size())
	}

	
	void testSubColumnQuery() {
		String cf = "Super1";
		
		TestCleanupDescriptor cleanup = insertSuperColumns(cf, 1, "testSubColumnQuery", 1,
				"testSubColumnQuerySuperColumn");
		
		// get value
		SubColumnQuery<String,String, String, String> q = createSubColumnQuery(cassandraService.keyspace(), se, se, se, se);
		q.setSuperColumn("testSubColumnQuerySuperColumn0").setColumn("c000").setColumnFamily(cf);
		QueryResult<HColumn<String, String>> r = q.setKey("testSubColumnQuery0").execute();
		assertNotNull(r);
		HColumn<String, String> c = r.get();
		assertNotNull(c);
		String value = c.getValue();
		assertEquals("v000", value);
		String name = c.getName();
		assertEquals("c000", name);
		
		// get nonexisting value
		q.setColumn("column doesn't exist");
		r = q.execute();
		assertNotNull(r);
		c = r.get();
		assertNull(c);
		
		// remove value
		deleteColumns(cleanup);
	}
	
	void testSubColumnQueryGroovy() {
		String cf = "Super1";

		TestCleanupDescriptor cleanup = insertSuperColumns(cf, 1, "testSubColumnQuery", 1,	"testSubColumnQuerySuperColumn");
		
		// get value
		assertEquals("v000",cassandraService.getValue("testSubColumnQuery0",cf,"testSubColumnQuerySuperColumn0","c000",String.class))
		
		// get nonexisting value
		assertNull(cassandraService.getValue("testSubColumnQuery0",cf,"testSubColumnQuerySuperColumn0","column doesn't exist",String.class))
		
		// remove value
		deleteColumns(cleanup);

		// validate removed finished		
		assertNull(cassandraService.getValue("testSubColumnQuery0",cf,"testSubColumnQuerySuperColumn0","c000",String.class))
	}

	void testMultigetSliceQuery() {
		String cf = "Standard1";
		
		TestCleanupDescriptor cleanup = insertColumns(cf, 4, "testMultigetSliceQuery", 3,
				"testMultigetSliceQueryColumn");
		
		// get value
		MultigetSliceQuery<String, String, String> q = createMultigetSliceQuery(cassandraService.keyspace(), se, se, se);
		q.setColumnFamily(cf);
		q.setKeys("testMultigetSliceQuery1", "testMultigetSliceQuery2");
		// try with column name first
		q.setColumnNames("testMultigetSliceQueryColumn1", "testMultigetSliceQueryColumn2");
		QueryResult<Rows<String, String, String>> r = q.execute();
		assertNotNull(r);
		Rows<String, String, String> rows = r.get();
		assertNotNull(rows);
		assertEquals(2, rows.getCount());
		Row<String, String, String> row = rows.getByKey("testMultigetSliceQuery1");
		assertNotNull(row);
		assertEquals("testMultigetSliceQuery1", row.getKey());
		ColumnSlice<String, String> slice = row.getColumnSlice();
		assertNotNull(slice);
		// Test slice.getColumnByName
		assertEquals("value11", slice.getColumnByName("testMultigetSliceQueryColumn1").getValue());
		assertEquals("value12", slice.getColumnByName("testMultigetSliceQueryColumn2").getValue());
		assertNull(slice.getColumnByName("testMultigetSliceQueryColumn3"));
		// Test slice.getColumns
		List<HColumn<String, String>> columns = slice.getColumns();
		assertNotNull(columns);
		assertEquals(2, columns.size());
		
		// now try with start/finish
		q = createMultigetSliceQuery(cassandraService.keyspace(), se, se, se);
		q.setColumnFamily(cf);
		q.setKeys("testMultigetSliceQuery3");
		q.setRange("testMultigetSliceQueryColumn1", "testMultigetSliceQueryColumn3", false, 100);
		r = q.execute();
		assertNotNull(r);
		rows = r.get();
		assertEquals(1, rows.getCount());
		for (Row<String, String, String> row2 : rows) {
			assertNotNull(row2);
			slice = row2.getColumnSlice();
			assertNotNull(slice);
			for (HColumn<String, String> column : slice.getColumns()) {
				if (!column.getName().equals("testMultigetSliceQueryColumn1")
				&& !column.getName().equals("testMultigetSliceQueryColumn2")
				&& !column.getName().equals("testMultigetSliceQueryColumn3")) {
					fail("A columns with unexpected column name returned: " + column.getName());
				}
			}
		}
		
		deleteColumns(cleanup);
	}

	void testMultigetSliceQueryGroovy() {
		String cf = "Standard1";
		
		TestCleanupDescriptor cleanup = insertColumns(cf, 4, "testMultigetSliceQuery", 4,
				"testMultigetSliceQueryColumn");
		
		// get value
		def result = cassandraService.getValuesSlice(["testMultigetSliceQuery1", "testMultigetSliceQuery2"],cf,["testMultigetSliceQueryColumn1", "testMultigetSliceQueryColumn2"],String.class)
		assertEquals(2,result.size())
		assertEquals(2,result["testMultigetSliceQuery1"].size())
		assertEquals(2,result["testMultigetSliceQuery2"].size())
		assertEquals("value11", result["testMultigetSliceQuery1"]["testMultigetSliceQueryColumn1"]);
		assertEquals("value12", result["testMultigetSliceQuery1"]["testMultigetSliceQueryColumn2"]);
		assertNull(result["testMultigetSliceQueryColumn3"]);

		// now try with start/finish
		result = cassandraService.getValuesSliceRange(["testMultigetSliceQuery3"],cf,"testMultigetSliceQueryColumn1", "testMultigetSliceQueryColumn3",String.class,false,100)
		assertNotNull(result)
		assertEquals(1,result.size())
		result=result["testMultigetSliceQuery3"]
		assertNotNull(result)
		assertEquals(3,result.size())
		assertEquals("value31",result["testMultigetSliceQueryColumn1"])
		assertEquals("value32",result["testMultigetSliceQueryColumn2"])
		assertEquals("value33",result["testMultigetSliceQueryColumn3"])
		
		deleteColumns(cleanup);
	}

	
		
	void testSliceQueryGroovy() {
		String cf = "Standard1";
		
		TestCleanupDescriptor cleanup = insertColumns(cf, 1, "testSliceQuery", 4, "testSliceQuery");
		
		// get value
		def result = cassandraService.getValuesSlice("testSliceQuery0",cf,["testSliceQuery1", "testSliceQuery2", "testSliceQuery3"],String.class)
		assertNotNull(result)
		assertEquals(1,result.size())
		result=result['testSliceQuery0']
		assertNotNull(result)
		assertEquals(3,result.size())
		assertEquals("value01", result["testSliceQuery1"]);
		assertEquals("value02", result["testSliceQuery2"]);
		assertEquals("value03", result["testSliceQuery3"]);
		
		// now try with start/finish
		// try reversed this time
		result = cassandraService.getValuesSliceRange("testSliceQuery0",cf,"testSliceQuery2", "testSliceQuery1",String.class,true,100)
		assertNotNull(result);
		assertEquals(1,result.size())
		result=result['testSliceQuery0']
		assertNotNull(result)
		assertEquals(2,result.size())
		assertEquals("value01", result["testSliceQuery1"]);
		assertEquals("value02", result["testSliceQuery2"]);
		
		deleteColumns(cleanup);
	}

	void testSliceQuery() {
		String cf = "Standard1";
		
		TestCleanupDescriptor cleanup = insertColumns(cf, 1, "testSliceQuery", 4, "testSliceQuery");
		
		// get value
		SliceQuery<String, String, String> q = createSliceQuery(cassandraService.keyspace(), se, se, se);
		q.setColumnFamily(cf);
		q.setKey("testSliceQuery0");
		// try with column name first
		q.setColumnNames("testSliceQuery1", "testSliceQuery2", "testSliceQuery3");
		QueryResult<ColumnSlice<String, String>> r = q.execute();
		assertNotNull(r);
		ColumnSlice<String, String> slice = r.get();
		assertNotNull(slice);
		assertEquals(3, slice.getColumns().size());
		// Test slice.getColumnByName
		assertEquals("value01", slice.getColumnByName("testSliceQuery1").getValue());
		assertEquals("value02", slice.getColumnByName("testSliceQuery2").getValue());
		assertEquals("value03", slice.getColumnByName("testSliceQuery3").getValue());
		// Test slice.getColumns
		List<HColumn<String, String>> columns = slice.getColumns();
		assertNotNull(columns);
		assertEquals(3, columns.size());
		
		// now try with start/finish
		q = createSliceQuery(cassandraService.keyspace(), se, se, se);
		q.setColumnFamily(cf);
		q.setKey("testSliceQuery0");
		// try reversed this time
		q.setRange("testSliceQuery2", "testSliceQuery1", true, 100);
		r = q.execute();
		assertNotNull(r);
		slice = r.get();
		assertNotNull(slice);
		assertEquals(2, slice.getColumns().size());
		for (HColumn<String, String> column : slice.getColumns()) {
			if (!column.getName().equals("testSliceQuery1")
			&& !column.getName().equals("testSliceQuery2")) {
				fail("A columns with unexpected column name returned: " + column.getName());
			}
		}
		
		deleteColumns(cleanup);
	}

		
	//TODO: groovy this...
	void testSuperSliceQuery() {
		String cf = "Super1";
		
		Mutator<String> m = createMutator(cassandraService.keyspace(), se);
		for (int j = 1; j <= 3; ++j) {
			@SuppressWarnings("unchecked")
					HSuperColumn<String, String, String> sc = createSuperColumn("testSuperSliceQuery" + j,
					Arrays.asList(createColumn("name", "value", se, se)), se, se, se);
			m.addInsertion("testSuperSliceQuery", cf, sc);
		}
		
		MutationResult mr = m.execute();
		assertTrue("Time should be > 0", mr.getExecutionTimeMicro() > 0);
		log.debug("insert execution time: {}", mr.getExecutionTimeMicro());
		
		// get value
		SuperSliceQuery<String, String, String, String> q = createSuperSliceQuery(cassandraService.keyspace(), se, se, se, se);
		q.setColumnFamily(cf);
		q.setKey("testSuperSliceQuery");
		// try with column name first
		q.setColumnNames("testSuperSliceQuery1", "testSuperSliceQuery2", "testSuperSliceQuery3");
		QueryResult<SuperSlice<String, String, String>> r = q.execute();
		assertNotNull(r);
		SuperSlice<String, String, String> slice = r.get();
		assertNotNull(slice);
		assertEquals(3, slice.getSuperColumns().size());
		// Test slice.getColumnByName
		assertEquals("value", slice.getColumnByName("testSuperSliceQuery1").getColumns().get(0)
				.getValue());
		
		// now try with start/finish
		q = createSuperSliceQuery(cassandraService.keyspace(), se, se, se, se);
		q.setColumnFamily(cf);
		q.setKey("testSuperSliceQuery");
		// try reversed this time
		q.setRange("testSuperSliceQuery1", "testSuperSliceQuery2", false, 2);
		r = q.execute();
		assertNotNull(r);
		slice = r.get();
		assertNotNull(slice);
		for (HSuperColumn<String, String, String> scolumn : slice.getSuperColumns()) {
			if (!scolumn.getName().equals("testSuperSliceQuery1")
			&& !scolumn.getName().equals("testSuperSliceQuery2")) {
				fail("A columns with unexpected column name returned: " + scolumn.getName());
			}
		}
		
		// Delete values
		for (int j = 1; j <= 3; ++j) {
			m.addDeletion("testSuperSliceQuery", cf, "testSuperSliceQuery" + j, se);
		}
		mr = m.execute();
		
		// Test after deletion
		r = q.execute();
		assertNotNull(r);
		slice = r.get();
		assertNotNull(slice);
		assertTrue(slice.getSuperColumns().isEmpty());
	}
	
	
	//TODO: groovy this...
	/**
	 * Tests the SubSliceQuery, a query on columns within a supercolumn
	 */
	void testSubSliceQuery() {
		String cf = "Super1";
		
		// insert
		TestCleanupDescriptor cleanup = insertSuperColumns(cf, 1, "testSliceQueryOnSubcolumns", 1,
				"testSliceQueryOnSubcolumns_column");
		
		// get value
		SubSliceQuery<String, String, String, String> q = createSubSliceQuery(cassandraService.keyspace(), se, se, se, se);
		q.setColumnFamily(cf);
		q.setSuperColumn("testSliceQueryOnSubcolumns_column0");
		q.setKey("testSliceQueryOnSubcolumns0");
		// try with column name first
		q.setColumnNames("c000", "c110", "c_doesn't_exist");
		QueryResult<ColumnSlice<String, String>> r = q.execute();
		assertNotNull(r);
		ColumnSlice<String, String> slice = r.get();
		assertNotNull(slice);
		assertEquals(2, slice.getColumns().size());
		// Test slice.getColumnByName
		assertEquals("v000", slice.getColumnByName("c000").getValue());
		
		// now try with start/finish
		q = createSubSliceQuery(cassandraService.keyspace(), se, se, se, se);
		q.setColumnFamily(cf);
		q.setKey("testSliceQueryOnSubcolumns0");
		q.setSuperColumn("testSliceQueryOnSubcolumns_column0");
		// try reversed this time
		q.setRange("c000", "c110", false, 2);
		r = q.execute();
		assertNotNull(r);
		slice = r.get();
		assertNotNull(slice);
		for (HColumn<String, String> column : slice.getColumns()) {
			if (!column.getName().equals("c000") && !column.getName().equals("c110")) {
				fail("A columns with unexpected column name returned: " + column.getName());
			}
		}
		
		// Delete values
		deleteColumns(cleanup);
		
		// Test after deletion
		r = q.execute();
		assertNotNull(r);
		slice = r.get();
		assertNotNull(slice);
		assertTrue(slice.getColumns().isEmpty());
	}
	
	//TODO: groovy this...
	void testMultigetSuperSliceQuery() {
		String cf = "Super1";
		
		TestCleanupDescriptor cleanup = insertSuperColumns(cf, 4, "testSuperMultigetSliceQueryKey", 3,
				"testSuperMultigetSliceQuery");
		
		// get value
		MultigetSuperSliceQuery<String, String, String, String> q = createMultigetSuperSliceQuery(cassandraService.keyspace(), se, se, se,
				se);
		q.setColumnFamily(cf);
		q.setKeys("testSuperMultigetSliceQueryKey0", "testSuperMultigetSliceQueryKey3");
		// try with column name first
		q.setColumnNames("testSuperMultigetSliceQuery1", "testSuperMultigetSliceQuery2");
		QueryResult<SuperRows<String, String, String, String>> r = q.execute();
		assertNotNull(r);
		SuperRows<String, String, String, String> rows = r.get();
		assertNotNull(rows);
		assertEquals(2, rows.getCount());
		SuperRow<String, String, String, String> row = rows.getByKey("testSuperMultigetSliceQueryKey0");
		assertNotNull(row);
		assertEquals("testSuperMultigetSliceQueryKey0", row.getKey());
		SuperSlice<String, String, String> slice = row.getSuperSlice();
		assertNotNull(slice);
		// Test slice.getColumnByName
		assertEquals("v001", slice.getColumnByName("testSuperMultigetSliceQuery1").getColumns().get(0)
				.getValue());
		assertNull(slice.getColumnByName("testSuperMultigetSliceQuery3"));
		
		deleteColumns(cleanup);
	}
	
	//TODO: groovy this...
	void testMultigetSubSliceQuery() {
		String cf = "Super1";
		
		// insert
		TestCleanupDescriptor cleanup = insertSuperColumns(cf, 3, "testMultigetSubSliceQuery", 1,
				"testMultigetSubSliceQuery");
		
		// get value
		MultigetSubSliceQuery<String, String, String, String> q = createMultigetSubSliceQuery(cassandraService.keyspace(), se, se, se, se);
		q.setColumnFamily(cf);
		q.setSuperColumn("testMultigetSubSliceQuery0");
		q.setKeys("testMultigetSubSliceQuery0", "testMultigetSubSliceQuery2");
		// try with column name first
		q.setColumnNames("c000", "c110");
		QueryResult<Rows<String, String, String>> r = q.execute();
		assertNotNull(r);
		Rows<String, String, String> rows = r.get();
		assertNotNull(rows);
		assertEquals(2, rows.getCount());
		Row<String, String, String> row = rows.getByKey("testMultigetSubSliceQuery0");
		assertNotNull(row);
		assertEquals("testMultigetSubSliceQuery0", row.getKey());
		ColumnSlice<String, String> slice = row.getColumnSlice();
		assertNotNull(slice);
		// Test slice.getColumnByName
		assertEquals("v000", slice.getColumnByName("c000").getValue());
		assertEquals("v100", slice.getColumnByName("c110").getValue());
		// Test slice.getColumns
		List<HColumn<String, String>> columns = slice.getColumns();
		assertNotNull(columns);
		assertEquals(2, columns.size());
		
		// now try with start/finish
		q = createMultigetSubSliceQuery(cassandraService.keyspace(), se, se, se, se);
		q.setColumnFamily(cf);
		q.setKeys("testMultigetSubSliceQuery0");
		q.setSuperColumn("testMultigetSubSliceQuery0");
		// try reversed this time
		q.setRange("c000", "c110", false, 2);
		r = q.execute();
		assertNotNull(r);
		rows = r.get();
		assertEquals(1, rows.getCount());
		for (Row<String, String, String> row2 : rows) {
			assertNotNull(row2);
			slice = row2.getColumnSlice();
			assertNotNull(slice);
			assertEquals(2, slice.getColumns().size());
			for (HColumn<String, String> column : slice.getColumns()) {
				if (!column.getName().equals("c000") && !column.getName().equals("c110")) {
					fail("A columns with unexpected column name returned: " + column.getName());
				}
			}
		}
		
		// Delete values
		deleteColumns(cleanup);
	}
	
	//TODO: groovy this...
	// If this fails... its because your not using opp
	void testRangeSlicesQuery() {
		String cf = "Standard1";
		
		TestCleanupDescriptor cleanup = insertColumns(cf, 4, "testRangeSlicesQuery", 3,
				"testRangeSlicesQueryColumn");
		
		// get value
		RangeSlicesQuery<String, String, String> q = createRangeSlicesQuery(cassandraService.keyspace(), se, se, se);
		q.setColumnFamily(cf);
		q.setKeys("testRangeSlicesQuery1", "testRangeSlicesQuery3");
		// try with column name first
		q.setColumnNames("testRangeSlicesQueryColumn1", "testRangeSlicesQueryColumn2");
		QueryResult<OrderedRows<String, String, String>> r = q.execute();
		
		assertNotNull(r);
		OrderedRows<String, String, String> rows = r.get();
		assertNotNull(rows);
		println("rows.getList() " + rows.getList())
		
		assertEquals(3, rows.getCount());
		Row<String, String, String> row = rows.getList().get(0);
		assertNotNull(row);
		assertEquals("testRangeSlicesQuery1", row.getKey());
		ColumnSlice<String, String> slice = row.getColumnSlice();
		assertNotNull(slice);
		// Test slice.getColumnByName
		assertEquals("value11", slice.getColumnByName("testRangeSlicesQueryColumn1").getValue());
		assertEquals("value12", slice.getColumnByName("testRangeSlicesQueryColumn2").getValue());
		assertNull(slice.getColumnByName("testRangeSlicesQueryColumn3"));
		// Test slice.getColumns
		List<HColumn<String, String>> columns = slice.getColumns();
		assertNotNull(columns);
		assertEquals(2, columns.size());
		
		// now try with setKeys in combination with setRange
		q.setKeys("testRangeSlicesQuery1", "testRangeSlicesQuery5");
		q.setRange("testRangeSlicesQueryColumn1", "testRangeSlicesQueryColumn3", false, 100);
		r = q.execute();
		assertNotNull(r);
		rows = r.get();
		assertEquals(3, rows.getCount());
		for (Row<String, String, String> row2 : rows) {
			assertNotNull(row2);
			slice = row2.getColumnSlice();
			assertNotNull(slice);
			assertEquals(2, slice.getColumns().size());
			for (HColumn<String, String> column : slice.getColumns()) {
				if (!column.getName().equals("testRangeSlicesQueryColumn1")
				&& !column.getName().equals("testRangeSlicesQueryColumn2")) {
					fail("A columns with unexpected column name returned: " + column.getName());
				}
			}
		}
		
		// Delete values
		deleteColumns(cleanup);
	}
	
	//TODO: groovy this...
	void testRangeSuperSlicesQuery() {
		String cf = "Super1";
		
		TestCleanupDescriptor cleanup = insertSuperColumns(cf, 4, "testRangeSuperSlicesQuery", 3,
				"testRangeSuperSlicesQuery");
		
		// get value
		RangeSuperSlicesQuery<String, String,String, String> q = createRangeSuperSlicesQuery(cassandraService.keyspace(), se, se, se, se);
		q.setColumnFamily(cf);
		q.setKeys("testRangeSuperSlicesQuery2", "testRangeSuperSlicesQuery3");
		// try with column name first
		q.setColumnNames("testRangeSuperSlicesQuery1", "testRangeSuperSlicesQuery2");
		QueryResult<OrderedSuperRows<String, String, String, String>> r = q.execute();
		assertNotNull(r);
		OrderedSuperRows<String, String, String, String> rows = r.get();
		assertNotNull(rows);
		assertEquals(2, rows.getCount());
		SuperRow<String, String, String, String> row = rows.getList().get(0);
		assertNotNull(row);
		assertEquals("testRangeSuperSlicesQuery2", row.getKey());
		SuperSlice<String, String, String> slice = row.getSuperSlice();
		assertNotNull(slice);
		// Test slice.getColumnByName
		assertEquals("v021", slice.getColumnByName("testRangeSuperSlicesQuery1").get(0).getValue());
		assertEquals("v022", slice.getColumnByName("testRangeSuperSlicesQuery2").get(0).getValue());
		assertNull(slice.getColumnByName("testRangeSuperSlicesQuery3"));
		
		// now try with setKeys in combination with setRange
		q.setKeys("testRangeSuperSlicesQuery0", "testRangeSuperSlicesQuery5");
		q.setRange("testRangeSuperSlicesQuery1", "testRangeSuperSlicesQuery3", false, 100);
		r = q.execute();
		assertNotNull(r);
		rows = r.get();
		assertEquals(4, rows.getCount());
		for (SuperRow<String, String, String, String> row2 : rows) {
			assertNotNull(row2);
			slice = row2.getSuperSlice();
			assertNotNull(slice);
			assertEquals(2, slice.getSuperColumns().size());
			for (HSuperColumn<String, String, String> column : slice.getSuperColumns()) {
				if (!column.getName().equals("testRangeSuperSlicesQuery1")
				&& !column.getName().equals("testRangeSuperSlicesQuery2")) {
					fail("A columns with unexpected column name returned: " + column.getName());
				}
			}
		}
		
		// Delete values
		deleteColumns(cleanup);
	}
	
	//TODO: groovy this...
	void testRangeSubSlicesQuery() {
		String cf = "Super1";
		
		TestCleanupDescriptor cleanup = insertSuperColumns(cf, 4, "testRangeSubSlicesQuery", 3,
				"testRangeSubSlicesQuery");
		
		// get value
		RangeSubSlicesQuery<String, String,String, String> q = createRangeSubSlicesQuery(cassandraService.keyspace(), se, se, se, se);
		q.setColumnFamily(cf);
		q.setKeys("testRangeSubSlicesQuery2", "testRangeSubSlicesQuery3");
		// try with column name first
		q.setSuperColumn("testRangeSubSlicesQuery1");
		q.setColumnNames("c021", "c111");
		QueryResult<OrderedRows<String, String, String>> r = q.execute();
		assertNotNull(r);
		OrderedRows<String, String, String> rows = r.get();
		assertNotNull(rows);
		assertEquals(2, rows.getCount());
		Row<String, String, String> row = rows.getList().get(0);
		assertNotNull(row);
		assertEquals("testRangeSubSlicesQuery2", row.getKey());
		ColumnSlice<String, String> slice = row.getColumnSlice();
		assertNotNull(slice);
		// Test slice.getColumnByName
		assertEquals("v021", slice.getColumnByName("c021").getValue());
		assertEquals("v121", slice.getColumnByName("c111").getValue());
		assertNull(slice.getColumnByName("c033"));
		
		// Delete values
		deleteColumns(cleanup);
	}
	
	//TODO: groovy this...
	void testCountQuery() {
		String cf = "Standard1";
		
		TestCleanupDescriptor cleanup = insertColumns(cf, 1, "testCountQuery", 10,
				"testCountQueryColumn");
		CountQuery<String, String> cq = createCountQuery(cassandraService.keyspace(), se, se);
		cq.setColumnFamily(cf).setKey("testCountQuery0");
		cq.setRange("testCountQueryColumn", "testCountQueryColumn999", 100);
		QueryResult<Integer> r = cq.execute();
		assertNotNull(r);
		assertEquals(Integer.valueOf(10), r.get());
		
		// Delete values
		deleteColumns(cleanup);
		
		// Try a non existing row, make sure it gets 0 (not exceptions)
		cq = createCountQuery(cassandraService.keyspace(), se, se);
		cq.setColumnFamily(cf).setKey("testCountQuery_nonexisting");
		cq.setRange("testCountQueryColumn", "testCountQueryColumn999", 100);
		r = cq.execute();
		assertNotNull(r);
		assertEquals(Integer.valueOf(0), r.get());
	}
	
	
	//TODO: groovy this...
	void testSuperCountQuery() {
		String cf = "Super1";
		
		TestCleanupDescriptor cleanup = insertSuperColumns(cf, 1, "testSuperCountQuery", 11,
				"testSuperCountQueryColumn");
		SuperCountQuery<String, String> cq = createSuperCountQuery(cassandraService.keyspace(), se, se);
		cq.setColumnFamily(cf).setKey("testSuperCountQuery0");
		cq.setRange("testSuperCountQueryColumn", "testSuperCountQueryColumn999", 100);
		QueryResult<Integer> r = cq.execute();
		assertNotNull(r);
		assertEquals(Integer.valueOf(11), r.get());
		
		// Delete values
		deleteColumns(cleanup);
	}
	
	//TODO: groovy this...
	void testSubCountQuery() {
		String cf = "Super1";
		
		TestCleanupDescriptor cleanup = insertSuperColumns(cf, 1, "testSubCountQuery", 1,
				"testSubCountQueryColumn");
		SubCountQuery<String, String, String> cq = createSubCountQuery(cassandraService.keyspace(), se, se, se);
		cq.setRange("c0", "c3", 100);
		QueryResult<Integer> r = cq.setColumnFamily(cf).setKey("testSubCountQuery0").
				setSuperColumn("testSubCountQueryColumn0").execute();
		assertNotNull(r);
		assertEquals(Integer.valueOf(2), r.get());
		
		// Delete values
		deleteColumns(cleanup);
	}
	
	//TODO: groovy this...
	private void deleteColumns(TestCleanupDescriptor cleanup) {
		Mutator<String> m = createMutator(cassandraService.keyspace(), se);
		for (int i = 0; i < cleanup.rowCount; ++i) {
			for (int j = 0; j < cleanup.columnCount; ++j) {
				m.addDeletion(cleanup.rowPrefix + i, cleanup.cf, cleanup.columnsPrefix + j, se);
			}
		}
		m.execute();
	}
	
	//TODO: groovy this...
	private TestCleanupDescriptor insertSuperColumns(String cf, int rowCount, String rowPrefix,
	int scCount, String scPrefix) {
		Mutator<String> m = createMutator(cassandraService.keyspace(), se);
		for (int i = 0; i < rowCount; ++i) {
			for (int j = 0; j < scCount; ++j) {
				@SuppressWarnings("unchecked")
						HSuperColumn<String, String, String> sc = createSuperColumn(
						scPrefix + j,
						Arrays.asList(createColumn("c0" + i + j, "v0" + i + j, se, se),
						createColumn("c1" + 1 + j, "v1" + i + j, se, se)), se, se, se);
				m.addInsertion(rowPrefix + i, cf, sc);
			}
		}
		m.execute();
		return new TestCleanupDescriptor(cf, rowCount, rowPrefix, scCount, scPrefix);
	}
	
	//TODO: groovy this...
	private TestCleanupDescriptor insertColumns(String cf, int rowCount, String rowPrefix,
	int columnCount, String columnPrefix) {
		Mutator<String> m = createMutator(cassandraService.keyspace(), se);
		for (int i = 0; i < rowCount; ++i) {
			for (int j = 0; j < columnCount; ++j) {
				m.addInsertion(rowPrefix + i, cf, createColumn(columnPrefix + j, "value" + i + j, se, se));
			}
		}
		MutationResult mr = m.execute();
		assertTrue("Time should be > 0", mr.getExecutionTimeMicro() > 0);
		log.debug("insert execution time: #{mr.getExecutionTimeMicro()}");
		log.debug(mr.toString());
		return new TestCleanupDescriptor(cf, rowCount, rowPrefix, columnCount, columnPrefix);
	}
}
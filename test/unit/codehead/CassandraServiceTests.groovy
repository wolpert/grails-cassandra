package codehead;

import grails.test.*

class CassandraServiceTests extends GrailsUnitTestCase {

    def cassandraService

    protected void setUp() {
        super.setUp()
        cassandraService = new CassandraService()
    }

    protected void tearDown() {
        super.tearDown()
    }

    void testSomething() {

    }
}

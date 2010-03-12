
beans = {
        cassandraService(codehead.CassandraService){
            servers=["localhost:9160"] // add more servers as needed
            defaultKeyspace="NedTest"
        }
}

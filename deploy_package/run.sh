echo $*
java -cp ./slf4j-api-1.7.25.jar:./slf4j-simple-1.7.25.jar:./classes:./pilosa-client-0.8.4.jar net.tekol.yuce.Importer $*

# Pilosa Test with Java Client

## Requirements

- JDK 8 (tested with OpenJDK)
- Maven (tested with 3.5.0)

## Building

```
mvn package
```

That should create `target/pilosa-tst-1.0-SNAPSHOT.jar`

## Running

`java -jar pilosa-tst-1.0-SNAPSHOT.jar PILOSA_SERVER_ADDRESS NUMBER_OF_BITS`

Imports `NUMBER_OF_BITS + 1` bits which have row IDs and column IDs set to index. E.g.,
```
0,0
1,1
...
100,100
```


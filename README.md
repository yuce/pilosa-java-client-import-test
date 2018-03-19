# Pilosa Test with Java Client

## Requirements

- JDK 8 (tested with OpenJDK)
- Maven (tested with 3.5.0)

## Building

```
make
```

## Running

```
cd deploy_package
./run.sh PILOSA_ADDR MAX_ROW_ID MAX_COLUMN_ID BATCH_SIZE random [THREAD_COUNT [SLICE WIDTH]]
```

`THREAD_COUNT` defaults to the number of CPUs and `SLICE_WIDTH` defaults to the default Pilosa server slice width.

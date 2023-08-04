# ziptransfer

A fast and safe way to transfer large number of small files from a bucket to another bucket on MinIO.

## usage
```
export SRC_ACCESS_KEY=minioadmin
export SRC_SECRET_KEY=minioadmin

export DEST_ACCESS_KEY=minioadmin
export DEST_SECRET_KEY=minioadmin

export DEST_BUCKET=destinationbucket
export SRC_BUCKET=sourcebucket

export DEST_SERVER=destination-host:port
export SRC_SERVER=source-host:port

./ziptransfer
```

This would start transferring data from source bucket to destination bucket

### tls
```
export DEST_SECURE="true"
```
```
export SRC_SECURE="true"
```

In-case if you have a TLS source and destination. TLS trust is not needed this tool automatically trusts all sites.

### internals

- The concurrency of downloads to create a zip archive is controlled via GOMAXPROCS=N (n is any positive integer, defaults to the number of CPUs on the node this tool runs on)
- Each zip archive is upto 100 entries (this value is not configurable however if you want I can allow configuring) but for now it defaults to 100 per zip archive
- You can also set `COMPRESS=true` to create a compressed archive (by default the archive uploaded to MinIO is not compressed)

## benchmarks

Total time taken with new zip transfer of 24643 objects all small files
```
export SRC_ACCESS_KEY=minioadmin
export SRC_SECRET_KEY=minioadmin
export DEST_ACCESS_KEY=minioadmin
export DEST_SECRET_KEY=minioadmin
export DEST_BUCKET=testbucket1
export DEST_SERVER=localhost:9001
export SRC_BUCKET=testbucket
export SRC_SERVER=localhost:9000

time ./ziptransfer

real    0m23.277s
user    0m5.510s
sys     0m1.831s
```

v/s `mc mirror` of the same content

```
mc mirror src/testbucket dest/testbucket1
...000/testbucket/zsync/copyright: 388.22 MiB / 388.22 MiB ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 3.83 MiB/s 1m41s
real    1m41.403s
user    6m34.531s
sys     0m22.039s
```

`ziptransfer` is 4x faster



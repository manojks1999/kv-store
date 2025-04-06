## how to run it locally

```shell
git clone git@github.com:manojks1999/kv-store.git
cd kv-store
```
### install dependencies
```shell
make install
```
### create necessary directories
```shell
make create-dirs
```

### start a 3 member cluster
```shell
make run-cluster
```

### stop a cluster 
```shell
make stop-cluster
```

### cleanup 
```shell
make clean
```

### test
```shell
make test
```
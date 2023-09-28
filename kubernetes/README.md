## Basic Usage (with kontemplate)

Install [kontemplate](https://code.tvl.fyi/tree/ops/kontemplate)

From within this directory

### Delete Pod and Restart

```sh
kontemplate delete values.yaml
kontemplate apply values.yaml
```

or use the make file

```shell
make hard-restart
```

to check status and observe logs:

```sh
kubectl get pods
kubectl logs -f [POD_NAME]
```

or, for convenience:

```shell
kubectl logs -f $( kubectl get pods | grep arak-indexer | awk '{print $1}')
```
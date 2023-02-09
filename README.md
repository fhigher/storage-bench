## 压测磁盘或存储写入效率
### cd storage-bench && go build
### ./storage-bench --h
    help info:
        --path  specify file store path, not filename
        --size  specify file size. the unit is GB, eg: --size=1, it is 1GB
        --con   specify the number of files generated concurrently
        --json  output json file
        --clean auto delete bench file
        --h    show this info 

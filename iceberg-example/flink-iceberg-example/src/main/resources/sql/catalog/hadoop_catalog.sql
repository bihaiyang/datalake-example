-- warehouse location metadata files and data files
-- warehouse 后面必须带有path,此处hadoop path包有bug, 会在后面拼上default db,但是因为是端口不会添加'/'
create catalog hadoop
with (
     'type'='iceberg',
     'catalog-type'='hadoop',
     'catalog-name'='hadoop',
     'warehouse'='alluxio://alluxio-master-test-0.default.svc.cluster.local:19998/iceberg/'
);




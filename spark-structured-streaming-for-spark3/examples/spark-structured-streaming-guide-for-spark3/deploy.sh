

PYSPARK_PYTHON=./environment/bin/python \
spark3-submit \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python \
--master yarn \
--deploy-mode cluster \
--principal username \
--keytab username.keytab \
--archives environment.tar.gz#environment \
--jars jar_files
--files cert.crt,schema.json \
script.py
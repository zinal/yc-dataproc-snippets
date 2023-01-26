#! /bin/sh
# Update /opt/conda image using the local repository.

CHANNEL="$1"

if [ -z "$CHANNEL" ]; then
    echo "NOTE: Conda repository URL was not specified, /opt/conda left as is"
    exit 0
fi

set -e
set -u

conda update -c ${CHANNEL} --override-channels --all --yes
conda install -c ${CHANNEL} --override-channels --yes conda-libmamba-solver
conda config --set solver libmamba
conda install -c ${CHANNEL} --override-channels --yes \
  'catboost>0' \
  'lightgbm>0' \
  'nltk>0' \
  'prophet>0' \
  'seaborn>0' \
  'unidecode>0' \
  'psycopg2>0'

# Patch Zeppelin 0.9.0 as part of Data Proc 2.0
if [ -f /usr/lib/zeppelin/interpreter/spark/python-interpreter-with-py4j-0.9.0.jar ]; then
# Download and replace the zeppelin_python.py file.
IMAGE2=`dirname "$IMAGE"`/zeppelin_python.py
DEST_PY=/usr/lib/zeppelin/interpreter/python/python/zeppelin_python.py
sudo -u hdfs hdfs dfs -copyToLocal "$IMAGE2" /tmp/zeppelin_python.py
mv ${DEST_PY} ${DEST_PY}-orig
mv /tmp/zeppelin_python.py ${DEST_PY}
chown root:root ${DEST_PY}
chmod 644 ${DEST_PY}
# Replace the copy in the jar library
cd /usr/lib/zeppelin/interpreter/spark
cp ${DEST_PY} python/
jar -uf spark-interpreter-0.9.0.jar python/zeppelin_python.py &
cd /usr/lib/zeppelin/interpreter/python
jar -uf python-interpreter-with-py4j-0.9.0.jar python/zeppelin_python.py
# Wait for completion
wait
fi

# End Of File

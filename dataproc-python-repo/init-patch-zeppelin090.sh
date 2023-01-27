#! /bin/sh
# Patch Zeppelin 0.9.0 as part of Data Proc 2.0

SOURCE="$1"
if [ -z "$SOURCE" ]; then
    echo "NOTE: Source path to zeppelin_python.py not specified, skipping action." >&2
    exit 0
fi

if [ ! -f /usr/lib/zeppelin/interpreter/spark/python-interpreter-with-py4j-0.9.0.jar ]; then
    echo "NOTE: Zeppelin 0.9.0 not found, skipping action." >&2
    exit 0
fi

# Download and replace the zeppelin_python.py file.
sudo -u hdfs hdfs dfs -copyToLocal "$SOURCE" /tmp/zeppelin_python.py
DEST_PY=/usr/lib/zeppelin/interpreter/python/python/zeppelin_python.py
mv ${DEST_PY} ${DEST_PY}-orig
mv /tmp/zeppelin_python.py ${DEST_PY}
chown root:root ${DEST_PY}
chmod 644 ${DEST_PY}

# Replace the copy in the jar libraries
cd /usr/lib/zeppelin/interpreter/spark
cp ${DEST_PY} python/
jar -uf spark-interpreter-0.9.0.jar python/zeppelin_python.py &
cd /usr/lib/zeppelin/interpreter/python
jar -uf python-interpreter-with-py4j-0.9.0.jar python/zeppelin_python.py
# Wait for completion
wait

# End Of File
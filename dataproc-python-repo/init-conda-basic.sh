#! /bin/sh
# Update /opt/conda image using the local repository.

CHANNEL="$1"

if [ -z "$CHANNEL" ]; then
    echo "NOTE: Conda repository URL was not specified, /opt/conda left as is"
    exit 0
fi

set -e
set -u

PATH=/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export PATH

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

# End Of File
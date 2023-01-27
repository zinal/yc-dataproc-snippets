#! /bin/sh
# Generates the local repository file structure based on /opt/conda environment.

REPO=/Mirror/conda1

mkdir -pv ${REPO}

(cd /opt/conda/pkgs && ls *.{conda,bz2}) | while read fname; do
  url=`grep "$fname" /opt/conda/pkgs/urls.txt`
  if [ ! -z "$url" ]; then
    part1=`dirname $url`
    arch=`basename $part1`
    mkdir -pv ${REPO}/$arch
    cp -v /opt/conda/pkgs/$fname ${REPO}/$arch/
  fi
done

conda index ${REPO}

find ${REPO} -type d -name .cache | sort -u | while read x; do rm -rf $x; done

# End Of File
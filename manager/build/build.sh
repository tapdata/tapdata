#!/usr/bin/env bash
ulimit -c unlimited

sbin_file="tm-*.jar"
mvn clean install -DskipTests || exit 1

rm -rf dist
mkdir -p dist dist/bin dist/lib dist/conf dist/logs

cp "tm/target/classes/logback.xml" dist/conf
cp "tm/target/classes/application.yml" dist/conf

f=`find tm/target/ -name $sbin_file` || exit 1
cp $f dist/lib

cp "build/start.sh" dist/bin

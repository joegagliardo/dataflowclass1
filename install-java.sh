#! /bin/sh
apt-get update -y
apt-get upgrade -y
apt-get install -y nano default-jdk
wget -q -nc -O gradle.zip https://services.gradle.org/distributions/gradle-5.0-bin.zip
rm -rf /opt/gradle*
unzip -q -d /opt gradle.zip
rm gradle.zip


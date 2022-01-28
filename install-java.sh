#! /bin/sh
apt-get update -y
apt-get upgrade -y
apt-get install -y default-jdk
cd /tmp
wget -q -nc -O gradle.zip https://services.gradle.org/distributions/gradle-5.0-bin.zip
unzip -q -d /opt gradle.zip'
rm gradle-5.0-bin.zip


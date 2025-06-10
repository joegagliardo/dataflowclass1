#! /bin/sh
echo 'alias docker-compose="docker compose --compatibility "$@""' >> ~/.bashrc
source ~/.bashrc # Or ~/.zshrc, ~/.profile depending on your shell
pip install "apache-beam[gcp]"


#!/bin/sh
echo " --> Executing custom shell script in vagrant env"

echo "Hello from $0"
python -V &> /dev/null && python -mplatform
arch
cd /vagrant && sudo python setup.py test && sudo python setup.py install
echo "------------------------------------------------"

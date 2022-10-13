Repo contains sample project with module for Kafka topic management, along with role and test playbook.

Implemented features:
- create topic, 
- list topics, 
- delete topic, 
- set specific amount of partitions on specific topic, 
- set retention on specific topic. 

Module assumes some reasonable defaults for testing:
- kafka-topics.sh on the $PATH, can be overriden by setting cmd_path parameter
- by default try to connect to local kafka server on localhost:9092, can be overriden by setting parameter bootstrap_server

For detailed instructions and examples look at library\kafka_manager.py

When applying remember commands need to be executed only on one Kafka GW host per cluster

Playbook can be executed with:
ansible-playbook -i inventory.yaml playbook.yaml

tests can be run:
ansible-playbook -i inventory.yaml test-playbook.yaml

Possible improvements:
- switch from kafka-topics.sh to API calls for better interoperatibility
- add authentication
- add better testing coverage

Attached playbook assume inventory contains some vars set for host, sample inventory file:

nodes:
  hosts:
    kafkagw:
  vars:
    cmd_path: /home/user/kafka_2.13-3.2.1/bin/kafka-topics.sh

Project tested on:
- Kafka 2.8.1
- Ansible [core 2.13.5]
- Python 3.10.6 

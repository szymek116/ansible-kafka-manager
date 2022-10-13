#!/usr/bin/python

from __future__ import (absolute_import, division, print_function)
from enum import Enum
from ansible.module_utils.basic import AnsibleModule
import subprocess
__metaclass__ = type

DOCUMENTATION = r'''
---
module: kafka_manager

short_description: This module is used to manage kafka resources: topics

version_added: "1.0.0"

description: Module for management of Kafka topics, including listing, creating and delteion

options:
    topic:
        description: name of the topics for configurator
        required: false
        type: str
    list:
        description: Flag indicating we are listing topics
        required: false
        type: bool
    create:
        description: Flag indicating we are creating topic
        required: false
        type: bool
    delete:
        description: Flag indicating we are deleting topic
        required: false
        type: bool
    bootstrap_server:
        description: address of bootstrap server
        default: localhost:9092
        required: false
        type: str
    partitions:
        description: number of partitions in the topic in case of creation
        required: false
        type: int
    retention_bytes:
        description: log retention expressed in bytes per partition!
        required: false
        type: int
    retention_ms:
        description: log retention express in ms
        required: false
        type: int
    cmd_path:
        description: path to the kafka-topics.sh
        default: "kafka-topics.sh"
        required: false
        type: str

Following restrictions apply for setting options:
required_if=[
    ('create', True, ('topic', 'partitions')),
    ('delete', True, ('topic',)),
],
mutually_exclusive=[
    ('list', 'create', 'delete'),
    ('list', ('topic', 'partitions', 'retention_bytes', 'retention_ms')),
    ('delete',  ('partitions', 'retention_bytes', 'retention_ms'))

author:
    - Artur Szymanski
'''

EXAMPLES = r'''
- name: Create topic stream_a
  kafka_manager:
    cmd_path: /home/arturszymanski/Desktop/solita/kafka_2.13-3.2.1/bin/kafka-topics.sh
    create: True
    topic: stream_a
    partitions: 3
    retention_bytes: 1000

- name: Create topic stream_b
  kafka_manager:
    cmd_path: /home/arturszymanski/Desktop/solita/kafka_2.13-3.2.1/bin/kafka-topics.sh
    create: True
    topic: stream_b
    partitions: 1
    retention_ms: 36000

- name: Delete topic stream_a
  kafka_manager:
    cmd_path: /home/arturszymanski/Desktop/solita/kafka_2.13-3.2.1/bin/kafka-topics.sh
    delete: True
    topic: stream_a

- name: List topics
  kafka_manager:
    cmd_path: /home/arturszymanski/Desktop/solita/kafka_2.13-3.2.1/bin/kafka-topics.sh
    list: True
  register: cmdout

- name: Print topics
  debug: msg="{{ cmdout.stdout_lines }}"
'''

RETURN = r'''
changed:
    description: Indicate if system was changed.
    type: bool
    returned: always
    sample: True
msg:
    description: short decription of cmd result
    type: str
    returned: always
    sample: "Topic exist: SampleTopic"
status:
    description: Indicates result of the command, can return OK and FAIL. OK include topic was not created because it exists
    type: str
    returned: always
    sample: 'OK'
stdout_lines:
    description: Preety printed msg
    type: str
    returned: only when list=True
    sample: [
        "__consumer_offsets",
        "connect-test8",
        "connect-test9",
        "stream_b",
        "testAns",
        "'"
    ]
'''


class KafkaManager:
    @property
    def ansible_module(self):
        return self.__ansible_module

    @ansible_module.setter
    def ansible_module(self, ansible_module):
        self.__ansible_module = ansible_module

    @property
    def topic_list_pre_cmd(self):
        return self.__topic_list_pre_cmd

    @topic_list_pre_cmd.setter
    def topic_list_pre_cmd(self, topic_list_pre_cmd):
        self.__topic_list_pre_cmd = topic_list_pre_cmd

    @property
    def topic_list_post_cmd(self):
        return self.__topic_list_post_cmd

    @topic_list_post_cmd.setter
    def topic_list_post_cmd(self, topic_list_post_cmd):
        self.__topic_list_post_cmd = topic_list_post_cmd

    @property
    def cmd_result(self):
        return self._cmd_result

    @cmd_result.setter
    def cmd_result(self, cmd_result):
        self._cmd_result = cmd_result

    def __init__(self, module):
        self.ansible_module = module

    def build_kafka_command(self, params=None):
        if not params:
            params = self.ansible_module.params
        exec_cmd = list([params["cmd_path"]])
        for k, v in params.items():
            match k:
                case "cmd_path":
                    continue
                case "list":
                    if v:
                        exec_cmd += ["--%s" % (k)]
                    continue
                case "create":
                    if v:
                        exec_cmd += ["--%s" % (k), "--if-not-exists"]
                    continue
                case "delete":
                    if v:
                        exec_cmd += ["--%s" % (k), "--if-exists"]
                    continue
                case "bootstrap_server" | "topic" | "partitions":
                    if v:
                        exec_cmd += ["--%s" % (k.replace("_", "-")), str(v)]
                    continue
                case _:
                    if v:
                        exec_cmd += [
                            "--config",
                            "%s=%s" % (k.replace("_", "."), str(v))
                        ]
                    continue
        return exec_cmd

    def get_topic_list(self):
        params = dict(
            list=True,
            bootstrap_server=self.ansible_module.params["bootstrap_server"],
            cmd_path=self.ansible_module.params["cmd_path"]
        )
        self.topic_list_pre_cmd = subprocess.run(
            self.build_kafka_command(params),
            capture_output=True).stdout

    def execute_cmd(self):
        self.get_topic_list()
        self.cmd_result = subprocess.run(
            self.build_kafka_command(), capture_output=True)
        self.check_results()

    def check_results(self):
        if self.cmd_result.returncode != 0:
            self.cmd_result = dict(
                status="FAIL",
                changed=False,
                msg=self.cmd_result.stderr
            )
            return

        if ("create", True) in self.ansible_module.params.items():
            if self.cmd_result.returncode == 0:
                if self.ansible_module.params["topic"] in str(
                        self.topic_list_pre_cmd):
                    self.cmd_result = dict(
                        status="OK", changed=False, msg="Topic exist: %s" %
                        (self.ansible_module.params["topic"]))
                else:
                    self.cmd_result = dict(
                        status="OK",
                        changed=True,
                        msg=self.cmd_result.stdout
                    )
        elif ("delete", True) in self.ansible_module.params.items():
            if self.cmd_result.returncode == 0:
                if self.ansible_module.params["topic"] in str(
                        self.topic_list_pre_cmd):
                    self.cmd_result = dict(
                        status="OK", changed=True, msg="Topic deleted: %s" %
                        (self.ansible_module.params["topic"]))
                else:
                    self.cmd_result = dict(
                        status="OK", changed=False, msg="Topic not present: %s" %
                        (self.ansible_module.params["topic"]))
        else:
            self.cmd_result = dict(
                status="OK",
                changed=False,
                msg=self.cmd_result.stdout,
                stdout_lines=str(self.cmd_result.stdout).split("\\n")
            )


def run_module():
    module_args = dict(
        topic=dict(type='str', required=False),
        list=dict(type='bool', required=False),
        create=dict(type='bool', required=False),
        delete=dict(type='bool', required=False),
        bootstrap_server=dict(type='str', required=False, default="localhost:9092"),
        partitions=dict(type='int', required=False),
        retention_bytes=dict(type='int', required=False),
        retention_ms=dict(type='int', required=False),
        cmd_path=dict(type='str', required=False, default="kafka-topics.sh"),
    )

    result = dict(
        changed=False,
        message=''
    )

    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True,
        required_if=[
            ('create', True, ('topic', 'partitions')),
            ('delete', True, ('topic',)),
        ],
        mutually_exclusive=[
            ('list', 'create', 'delete'),
            ('list', ('topic', 'partitions', 'retention_bytes', 'retention_ms')),
            ('delete', ('partitions', 'retention_bytes', 'retention_ms'))
        ]
    )

    if module.check_mode:
        module.exit_json(**result)

    km = KafkaManager(module)
    km.execute_cmd()
    result = km.cmd_result

    if result['status'] == "OK":
        module.exit_json(**result)
    else:
        module.fail_json(**result)


def main():
    run_module()


if __name__ == '__main__':
    main()

"""
Version 6. ES monitoring for some messaging and ussd platfrom.
Request to new ES cluster, save results into a file,
check content with regexps and send information to zabbix.
Added Logging and some improvements.
Added logic for recovery messages using pyzabbix library.
"""

import elasticsearch
from elasticsearch import Elasticsearch
import os
import re
import logging
from pyzabbix import ZabbixAPI, ZabbixAPIException
import datetime
from time import sleep
import subprocess
import sys
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

__author__ = 'itymoshenko'

# Global variables
log_error = "/var/log/elasticsearch/error.log"
log_file = "/var/log/elasticsearch/esmon.log"
prod_hosts = ["test1.int", "test2.int"]
zab_conf = "/etc/zabbix/zabbix_agentd.conf"

# Errors dicts
# Need to add \ before | for correct work
msg_dict = (
    {"key": "Failed_read_write_HTTP",
     "value": "^.*\|[0-9]*\|Failed to read/write io streams, when executing HTTP.*$"},
    {"key": "SMPP_unable_send_message",
     "value": "^.*\|[0-9]*\|*Unable send message with code.*because error:.*$"},
    {"key": "Memcached_operation_timeout", "value": "^.*\|[0-9]*\|Memcached operation.*timeout happened.*$"},
    {"key": "Parallel_requests_limit",
     "value": "^.*\|[0-9]*\|Attention exceeded the value window when send a message. "
              "The current number of parallel processed requests:.*$"}
)

ussd_dict = (
    {"key": "Dest_addr_doesnt_registered",
     "value": "^.*\|[0-9]*\|Dest addr.*doesn't registered for USSD CHANNEL.*$"},
    {"key": "Failed_read_write_HTTP",
     "value": "^.*\|[0-9]*\|Failed to read/write io streams, when executing HTTP.*method using url .*, because .*$"}
)

# Logging settings
logger = logging.getLogger("Log format")
logger.setLevel(logging.INFO)
handler = logging.FileHandler(log_file)
formatter = logging.Formatter("%(asctime)s:%(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def check_file():
    """ Check if log_error file exist and remove him with new executing of the script """
    if os.path.exists(log_error):
        os.remove(log_error)
    else:
        logger.info("Sorry, I can not remove %s file. This file doesn't exist." % log_error)


check_file()


def output(arg):
    """ Create file to collect output data
    :param arg: data received from ES request
    :return: structured data in file
    """
    f = open(log_error, 'a')
    errors = "{0}".format(arg)
    f.write(errors + '\n')
    f.close()


# Connect to ElasticSearch balancer
es = Elasticsearch([{'host': 'x.x.x.x', 'port': 9200}])

# Check availability of ElasticSearch
try:
    es.ping()
except elasticsearch.ConnectionError:
    logger.info("Connection to ElasticSearch failed! "
                "Please check it manually!")
    logger.info("Further execution of the script is suspended!")
    logger.info("#" * 70)
    sys.exit(0)


def request(*args):
    """ Sending request to ElasticSearch balancer
    :param args: hosts from list
    :return: aggregation information about errors for hosts in list
    """
    response = es.search(
        index="{0}*".format(*args),
        body={
            "size": 0,
            "query": {
                "filtered": {
                    "query": {
                        "match_all": {}
                    },
                    "filter": {
                        "bool": {
                            "must": [
                                {
                                    "range": {
                                        "@timestamp": {
                                            "gte": "now-1m"
                                        }
                                    }
                                }
                            ],
                            "must_not": []
                        }
                    }
                }
            },
            "aggs": {
                "errmsg": {
                    "terms": {
                        "field": "errmsg",
                        "size": 20,
                        "order": {
                            "_count": "desc"
                        }
                    },
                    "aggs": {
                        "code": {
                            "terms": {
                                "field": "code",
                                "size": 20,
                                "order": {
                                    "_count": "desc"
                                }
                            }
                        }
                    }
                }
            }
        })

    total_count = response['hits']['total']
    hostname = "{0}".format(*args)
    logger.info("Host: %s." % hostname)
    logger.info("Total count of errors: %s" % total_count)

    try:
        for i in response['aggregations']['errmsg']['buckets']:
            count = str(int(i['doc_count']))
            error_message = str(i['key'])
            code = i['code']['buckets']
            final_code = ''.join(x['key'] for x in code)
            data = "{0}|{1}|{2}|{3}".format(hostname, count, error_message, final_code)
            logger.info(data)
            output(data)
    except KeyError:
        pass
    logger.info("-" * 50)


for host in prod_hosts:
    request(host)


def zabbix_sender(*args):
    """ Send information to zabbix
    :param args: zab_test, host_name, key, final_value
    :return: run zabbix_sender via command line and send information
    """
    send = '/usr/bin/zabbix_sender -c {0} -s {1} -k {2} -o "{3}"'.format(*args)
    logger.info("Send info to zabbix:")
    logger.info(send)
    command = subprocess.Popen(send, shell=True, stdout=subprocess.PIPE)
    out = command.stdout.read()
    logger.info(out)
    logger.info("-" * 50)


# Check regex from the dicts
try:
    with open(log_error, 'r') as log_file:
        for line in log_file:
            for z in msg_dict:
                var_msg = re.compile(z['value'])
                matches = re.search(var_msg, line)
                if matches:
                    h = re.findall(r'\w*-\w*.int', line)
                    host_name = ''.join(h)
                    value = line.split(host_name, 1)[1]
                    final_value = (value.strip("|")).rstrip()
                    key = z['key']
                    if host_name.startswith(("msg")):
                        zabbix_sender(zab_conf, host_name, key, final_value)
            for q in ussd_dict:
                var_ussd = re.compile(q['value'])
                matches = re.search(var_ussd, line)
                if matches:
                    h = re.findall(r'\w*-\w*.int', line)
                    host_name = ''.join(h)
                    value = line.split(host_name, 1)[1]
                    final_value = (value.strip("|")).rstrip()
                    key = q['key']
                    if host_name.startswith("ussd"):
                        zabbix_sender(zab_conf, host_name, key, final_value)
except IOError:
    logger.info("An error occurred while opening log error file. "
                "Seems file not exist! "
                "Then there is no errors for last check!")

sleep(10)

# Connect to Zabbix via API
# Added for disable warning: InsecureRequestWarning: Unverified HTTPS request is being made.
# Adding certificate verification is strongly advised.
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

try:
    # The hostname at which the Zabbix web interface is available
    ZAB_SERVER = 'https://zabbix-ui.int'
    zab_api = ZabbixAPI(ZAB_SERVER)
    zab_api.session.verify = False
    # Login to the Zabbix API
    zab_api.login("user", "pass")
except ZabbixAPIException as api_error:
    logger.info("Connect to Zabbix via API:")
    logger.info(api_error)
    sys.exit(1)
except requests.exceptions.HTTPError as http_error:
    logger.info("Connect to Zabbix via API:")
    logger.info(http_error)
    sys.exit(1)

# Send recovery messages to zabbix
logger.info("Send recovery messages for active problems in zabbix: ")


def recovery_actions(*args):
    """ Checking last change time in active triggers
    and sending recovery messages to zabbix for resolving current problems.
    """
    last_change = "{1}".format(*args)
    last_change_time = datetime.datetime.fromtimestamp(float(last_change))
    logger.info("Hostname: {0} ID: {1}".format(host_name, hid)) 
    logger.info("Issue: {0}".format(*args))
    logger.info("Last change time: {0}".format(last_change_time))
    logger.info("Unacknowledged events count: {0}".format(len(events)))
    time_now = datetime.datetime.now()
    timedelta = ((time_now - last_change_time).total_seconds() / 60.0)
    recovery_interval = 30  # minutes
    if timedelta >= recovery_interval:
        if len(events) > 0:
            for event in events:
        #       logger.info(event) # for diagnostic
                event_id = event['eventid']
                zab_api.event.acknowledge(eventids=event_id, message="Event closed by script")
            logger.info("All events has been acknowledged now!")
        recovery = "Recovery message"
        hostname = "{2}".format(*args)
        keys = "{0}".format(*args)
        zabbix_sender(zab_conf, hostname, keys, recovery)
    elif timedelta < recovery_interval:
        logger.info("Recovery message will be sent in {0} minutes!".format(recovery_interval))
        logger.info("-" * 50)


key_list = []

for key in msg_dict:
    key_list.append(key['key'])

for key in ussd_dict:
    key_list.append(key['key'])

counter = 0

for host in prod_hosts:
    hosts = zab_api.host.get(output=["hostid"], filter={"host": host})
    hid = hosts[0]['hostid']

    triggers = zab_api.trigger.get(filter={'host': host, 'value': 1}, 
                                   output='extend',
                                   monitored=1,
                                   active=1,
                                   expandExpression=1,
                                   expandDescription=1)

    if not triggers:
        counter += 1
        # logger.info("Hostname: {0} ID: {1}".format(host, hid))
        if counter == len(prod_hosts):
            logger.info("There is no active problems!")
            logger.info("-" * 50)
    else:
        for t in triggers:
            key = t['description']
            value = t['lastchange']
            tid = t['triggerid']
            host_name = host
            if key in key_list:
                events = zab_api.event.get(filter={'objectid': tid},
                                           output="extend",
                                           acknowledged=0,
                                           value=1,
                                           source=0)
                recovery_actions(key, value, host_name, hid)

logger.info("#" * 70)



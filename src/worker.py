# coding: utf-8

import json
import logging
import argparse
from socket import socket, AF_INET, SOCK_STREAM
import sys
from utils import reduce
import time

logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('worker')

## A worker process lifecycle
# Essentially, it receives messages from the coordinator telling it what sort of work he should do
# Furthermore, if the coordinator dies, he receives their backup address and attempts to connect to it
def main(args):
    logger.debug('Connecting to %s:%d', args.hostname, args.port)
    sock = socket(AF_INET, SOCK_STREAM)
    sock.connect((args.hostname, args.port))

    logger.debug('Connected to %s:%d', args.hostname, args.port)
    json_msg = json.loads('{"task":"register", "id":'+str(args.port)+'}')

    sock.send(json.dumps(json_msg).encode('UTF-8'))
    backup = None

    while True:

        req = sock.recv(8000000) # test for different size values
        timeout = 0

        if not req:
            time.sleep(4)

            if backup != None:

                while True:
                    try:
                        if timeout >= 60: # try to connect to backup
                            logger.info("Timed out - giving up connection..")
                            sock.close()
                            return
                        logger.debug('Connecting to %s:%d', backup[0], backup[1])
                        sock = socket(AF_INET, SOCK_STREAM)
                        sock.connect((backup[0],backup[1]))

                        logger.debug('Connected to %s', (backup[0],backup[1]))
                        json_msg = json.loads('{"task":"register", "id":'+str(backup[1])+'}') # TODO: Fix this id thing

                        sock.send(json.dumps(json_msg).encode('UTF-8'))
                        backup = None # if we get here it means we already connected to the backup
                        break
                    except:
                        logger.info("Coordinator died, attempting to reconnect")
                        time.sleep(1)
                        timeout = timeout + 1

            else:
                while True:
                    try:
                        if timeout >= 60: # try and connect to home addr
                            logger.info("Timed out - giving up connection..")
                            sock.close()
                            return
                        logger.debug('Connecting to %s:%d', args.hostname, args.port)
                        sock = socket(AF_INET, SOCK_STREAM)
                        sock.connect((args.hostname,args.port))

                        logger.debug('Connected to %s', (args.hostname, args.port))
                        json_msg = json.loads('{"task":"register", "id":'+str(8765)+'}') # TODO: Fix this id thing

                        sock.send(json.dumps(json_msg).encode('UTF-8'))
                        backup = None # if we get here it means we already connected to the backup
                        break
                    except:
                        logger.info("Coordinator died, attempting to reconnect")
                        time.sleep(1)
                        timeout = timeout + 1

        else:
            decoded=req.decode()
            str_req = json.loads(decoded)

            if str_req['task']=='map_request':
                logger.debug('Received a map_request, sending a map reply')
                blob = str_req['blob']
                map = []

                for w in blob:
                    if w:
                        map.append((w,1))

                j = {"task":"map_reply","value":map}
                json_msg=json.dumps(j).encode('latin-1')

                sock.send(json_msg)

            elif str_req['task']=='reduce_request':
                logger.debug('Received a reduce_request sending a reduce reply')
                val = reduce(str_req['value'][0], str_req['value'][1])
                j = {"task":"reduce_reply","value":val}
                json_msg=json.dumps(j).encode('latin-1')

                sock.send(json_msg)

            elif str_req['task']=='backup_update':
                backup = str_req['c']
                logger.info("Acknowledged the backup address")

            else:
                logger.debug('Error: Unrecognized operation')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MapReduce worker')
    parser.add_argument('--port', dest='port', type=int, help='coordinator port', default=8765)
    parser.add_argument('--hostname', dest='hostname', type=str, help='coordinator hostname', default='localhost')
    args = parser.parse_args()

    main(args)

# coding: utf-8
# @package coordinator
# Responsible for distributing work between other slave processes
# Syncing with the backup is done through short, heartbeat-like messages, containing the backup data.


import json
import sys
import csv
import selectors
import logging
import argparse
import time
import re
from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR
from _thread import *
from queue import Queue
import threading
from utils import getKey, Diff, tokenizer

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('coordinator')

# Array of usable blobs
wordList = []

# Array of lists
listList = []
index = -1

# Array of workers, used to iterate and broadcast msgs (like backup addresses)
workers = []

# Worker queue used to request work
worker_Q = Queue()

# Inbox queue used for incoming messages
recv_q = Queue()

# We are using a queue to store backups
# and not just a var in case we wish to scale to multiple backups later
backup_Q = Queue()

# Dictionary holding the latest request
# and to who it was requested, to allow error recovery
request_table = {}

# List of requests that were never returned (unprocessed)
unprocs = []

# Thread that will handle receiving messages from connection c


def recv_msg(c):
    global backup_Q
    global worker_Q
    global request_table
    global unprocs
    global listList
    global wordList
    global index
    first_heartbeat = True
    id = -1         # local variable that holds the id of this thread's connection

    while True:

        try:
            data = c.recv(40960000)
        except:
            data = False

        if not data:
            logger.debug('Worker %s died', id)
            if len(request_table) > 0 and id != 'BACKUP':
                unprocs.append(request_table.pop(id))
                logger.debug(
                    'Added a request to unprocs (size: %s) from Worker %s', len(unprocs), id)
            break

        try:

            str_req = json.loads(data)

        except:

            logger.error('Couldnt load data')
            continue

        if str_req['task'] == 'size':
            data = c.recv(str_req['size'])
            str_req = json.loads(data)

        if str_req['task'] == 'register':
            logger.debug('Registering a new worker')
            workers.append(c)
            # id is assigned by the coordinator and its the index of the new worker
            id = workers.index(c)
            worker_Q.put(c)

            # Tell the new worker about the backup (if it exists)
            if backup_Q.qsize() > 0:
                tpl = backup_Q.get()
                j = {'task': 'backup_update', 'c': tpl}
                json_msg = json.dumps(j).encode('latin-1')
                c.send(json_msg)
                backup_Q.put(tpl)

        elif str_req['task'] == 'backup_register':
            logger.debug('Registering a new backup coordinator')
            tpl = (str_req['addr'], str_req['port'])
            id = 'BACKUP'
            # @note
            # Backup has to know:
            # state of wordList (index), unprocessed requests, state of listList
            j = {'task': 'reg_ack', 'index': index, 'listList': listList}
            json_msg = json.dumps(j).encode('latin-1')
            c.send(json_msg)
            backup_Q.put(tpl)

            # Telling workers about the backup
            j = {'task': 'backup_update', 'c': tpl}
            json_msg = json.dumps(j).encode('latin-1')

            # Broadcasting the backup address
            for w in workers:
                w.send(json_msg)

            # Instead of using heartbeats, we will send requests and theyll work like heartbeats
            global reply_q
            reply_q = Queue()

        elif str_req['task'] == 'map_reply':
            logger.debug('Received a Map Reply')
            listList.append(str_req['value'])

            # if we are the backup handling this msg on different ends
            if id != 'BACKUP' and id != -1:
                str_req['index'] = index
                worker_Q.put(c)
                if backup_Q.qsize() > 0:
                    reply_q.put(str_req)
                    logger.debug('Sending backup data to backup')
            elif id == -1:
                j = {'task': 'heartbeat'}
                index = str_req['index']
                logger.debug('INDEX: %s', index)
                json_msg = json.dumps(j).encode('latin-1')
                sent = c.send(json_msg)

        elif str_req['task'] == 'reduce_reply':
            logger.debug('Received a Reduce Reply')

            global recv_q

            if id != 'BACKUP' and id != -1:
                # we need to use a queue here since were popping and appending items from the same list
                recv_q.put(str_req['value'])
                worker_Q.put(c)
                if backup_Q.qsize() > 0:
                    reply_q.put(str_req)
                    logger.debug('Sending backup data to backup')

            elif id == -1:
                listList.pop(0)
                listList.pop(0)

                listList.append(str_req['value'])
                logger.debug(
                    'receiving backup data from parent; updated list size: %s', len(listList))
                j = {'task': 'heartbeat'}

                json_msg = json.dumps(j).encode('latin-1')
                sent = c.send(json_msg)

        elif str_req['task'] == 'reg_ack' or str_req['task'] == 'heartbeat':

            if id == 'BACKUP':  # here we will send a message containing valuable backup data
                logger.debug('Received a heartbeat from the backup')
                j = reply_q.get()

            else:               # here we will receive a message containing valuable backup data
                logger.debug('Received an ack from my parent')
                logger.debug('ID: %s', id)

                j = {'task': 'heartbeat'}

            json_msg = json.dumps(j).encode('latin-1')
            sent = c.send(json_msg)

    global break_flag   # flag that will allow the backup to start working
    break_flag = True

    c.close()

# Thread that will distribute requests between workers
# it will handle blob-splitting and distributing the blobs
# to be replaced


def work():

    start_time = time.time()

    # iterate over the array of blobs and send it to a connection
    global worker_Q
    global listList
    global request_table
    global index
    global wordList
    logger.debug('STARTING THE WORK: INDEX: %s', index)

    if index > -1:
        i = index
    else:
        i = 0

    # MAP #

    while i < len(wordList):
        index = i
        b = wordList[i]
        i += 1

        while True:
            try:

                j = {"task": "map_request", "blob": b}
                json_msg = json.dumps(j).encode('latin-1')

                id = -1
                c = worker_Q.get()
                # identifying to whom were shipping work to
                id = workers.index(c)

                c.send(json_msg)
                request_table[id] = j

                logger.debug('Map %s Requested to worker %s', b, id)
                break

            except:

                logger.debug(
                    'Worker died, sending it to the next available worker...')

    while len(unprocs) != 0:
        logger.debug('Processing the unhandled map requests...')
        while True:
            try:
                req = unprocs.pop()
                json_msg = json.dumps(req).encode('latin-1')

                id = -1
                c = worker_Q.get()
                # identifying to whom were shipping work to
                id = workers.index(c)
                request_table[id] = req

                c.send(json_msg)

                logger.debug('Map Requested to worker %s', id)
                break

            except:

                logger.debug(
                    'Worker died, sending it to the next available worker...')

    request_table.clear()

    logger.debug('Starting the reduce.. %s, size of listlist: %s',
                 index, len(listList))
    time.sleep(3)
    firstIter = True
    to = 15  # timeout depends on list size
    if len(listList) > 1000:
        to = 30

    # REDUCE #

    while True:
        # send the first two items to the next worker
            # remove the first two items
            # send them
            # wait for a reduce reply

        if len(listList) > 1 or firstIter:

            if firstIter and len(listList) <= 1:
                # some times we would get here without receiving the map_reply
                time.sleep(1)

                firstIter = False
                while True:
                    try:
                        if len(listList) == 1:
                            # reducing just the first and only list item
                            value = [listList.pop(0), []]
                            j = {"task": 'reduce_request', "value": value}
                            json_msg = json.dumps(j).encode('latin-1')

                            id = -1
                            c = worker_Q.get()

                            # identifying to whom were shipping work to
                            id = workers.index(c)

                            c.send(json_msg)
                            request_table[id] = j

                            logger.debug('Reduce Requested to worker %s', id)
                            break
                    except:
                        logger.debug('Trying to send it to the next worker')
                        time.sleep(1)

            else:

                while True:
                    try:
                        if len(listList) > 1:
                            value = [listList.pop(0), listList.pop(0)]
                            j = {"task": 'reduce_request', "value": value}
                            json_msg = json.dumps(j).encode('latin-1')

                            id = -1
                            c = worker_Q.get()

                            # identifying to whom were shipping work to
                            id = workers.index(c)

                            c.send(json_msg)
                            request_table[id] = j

                            logger.debug('Reduce Requested to worker %s', id)
                            break
                        else:
                            break
                    except:
                        logger.debug('Trying to send it to the next worker')
                        time.sleep(6)

        # updating our list with queue contents (this doesnt work very well since were gonna have to wait for a reply every time we send a message)
        try:
            # timeout value varies with list size
            listList.append(recv_q.get(timeout=to))

            logger.debug("Size of updated list: %s", len(listList))

            # if updated list has only one item and we have no messages break
            if len(listList) == 1 and recv_q.qsize() == 0:
                break

        except:
            logger.debug("Worker request timed out")
            try:
                unprocs.append(request_table.pop(id))
            except:
                logger.info('Not adding anything to unprocs')

            if len(listList) == 0:  # if list is now empty cuz of the time out
                break

    request_table.clear()

    for r in unprocs:   # adding unprocessed items to the list of lists
        listList.append(r["value"][0])
        listList.append(r["value"][1])

    logger.debug(
        'Handling unprocessed requests: size of updated list is now %s', len(listList))

    while len(unprocs) != 0:  # works like a while True + if clause
        if len(listList) > 1:
            while True:
                try:
                    if len(listList) > 1:
                        value = [listList.pop(0), listList.pop(0)]
                        j = {"task": 'reduce_request', "value": value}
                        json_msg = json.dumps(j).encode('latin-1')

                        id = -1
                        c = worker_Q.get()

                        # identifying to whom were shipping work to
                        id = workers.index(c)

                        c.send(json_msg)
                        request_table[id] = j

                        logger.debug('Reduce Requested to worker %s', id)
                        break
                    else:
                        break
                except:
                    logger.debug('Trying to send it to the next worker')
                    time.sleep(3)

        # updating our list with queue contents (this doesnt work very well since were gonna have to wait for a reply every time we send a message)
        try:
            listList.append(recv_q.get(timeout=to))

            logger.debug("Size of updated list: %s", len(listList))

            # if updated list has only one item and we have no messages break
            if len(listList) == 1 and recv_q.qsize() == 0:
                break

        except:
            logger.debug("Worker request timed out")
            unprocs.append(request_table.pop(id))
            # if len(listList) == 0: # if list is now empty cuz of the time out
            #     break

    request_table.clear()

    logger.debug("Reached end of work - hooray!")
    elapsed_time = time.time() - start_time

    logger.debug(listList)

    # Writing to a csv

    fileName = ''.join(sys.argv[2])
    fileName = fileName[:-3]+'csv'

    with open(fileName, 'w') as result:
        writer = csv.writer(result, dialect='excel')
        for l in listList[0]:
            writer.writerow(l)

    logger.info("Elapsed time: %s; Length of the final list: %s",
                elapsed_time, len(listList[0]))


def main(args):
    datastore = []  # setting up the blobs
    with args.file as f:
        while True:
            blob = f.read(args.blob_size)
            if not blob:
                break
            # This loop is used to not break words in half
            while not str.isspace(blob[-1]):
                ch = f.read(1)
                if not ch:
                    break
                blob += ch
            #logger.debug('Blob: %s', blob)
            datastore.append(blob)

    global wordList
    for i in datastore:
        tokens = tokenizer(i)
        wordList.append(tokens)

    # create an INET, STREAMing socket
    serversocket = socket(AF_INET, SOCK_STREAM)
    ideal_port = args.port
    backup = False

    while True:

        try:
            serversocket.bind(('localhost', ideal_port))
            logger.info('Becoming a server socket on %s',
                        ('localhost', ideal_port))
            serversocket.listen(100)
            break

        except:
            # some times we enter here because of zombie processes
            logger.info(
                "Couldnt connect to that port, becoming a backup server")
            ideal_port = ideal_port - 1
            backup = True

    if backup == False:  # will enter here only if its not a backup coord

        start_new_thread(work, ())

    else:

        parentSocket = socket(AF_INET, SOCK_STREAM)
        parentSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        parentSocket.connect(('localhost', ideal_port+1))

        j = {'task': 'backup_register', 'addr': 'localhost', 'port': ideal_port}
        json_msg = json.dumps(j).encode('latin-1')

        parentSocket.send(json_msg)

        global break_flag
        break_flag = False

        start_new_thread(recv_msg, (parentSocket,))

        while True:
            if break_flag:
                break

        # when we get here it means the parent is dead
        parentSocket.close()

        logger.debug(
            'Parent coord died - taking all his connections and starting over')

        serversocket = socket(AF_INET, SOCK_STREAM)
        serversocket.bind(('localhost', ideal_port))
        # ideally this wouldnt be the port
        logger.info('Becoming a server socket on %s',
                    ('localhost', ideal_port))
        serversocket.listen(100)
        start_new_thread(work, ())

    # mainloop for the main Coord
    while True:

        c, addr = serversocket.accept()

        print('Connected to :', addr[0], ':', addr[1])

        # Start a new thread to listen to this new connection
        start_new_thread(recv_msg, (c,))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MapReduce Coordinator')
    parser.add_argument('-p', dest='port', type=int,
                        help='coordinator port', default=8765)
    parser.add_argument('-f', dest='file',
                        type=argparse.FileType('r'), help='file path')
    parser.add_argument('-b', dest='blob_size', type=int,
                        help='blob size', default=1024)
    args = parser.parse_args()

    main(args)

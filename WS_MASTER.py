import threading
import time
import os
import traceback
import socket
import heapq # https://docs.python.org/2/library/heapq.html
import json
import datetime
from queue import Queue
from websocket import create_connection

import TarAndSendToServer


BASE_PATH = "C:\\Users\\carterj3\\Desktop\\Hadoop\\Showdown\\"

utf8stdout = open(1, 'w', encoding='utf-8', closefd=False)

VERBOSE = True
START_TIME = time.time()
BYTES_RECV = 0.0

CONNS = []
RECV_THREADS = []
SEND_QUEUE = Queue(200)
SEND_QUEUE.put("|/cmd roomlist")

RECV_QUEUE = Queue(200)
ROOMS = []
JOINED_ROOMS = []

THREAD_ID = 0

PS_URL = "ws://sim.smogon.com:8000/showdown/websocket"

LAST_DATE = "2014-10-30"

def split_room_into_parts(room):
    revd_parts = room[::-1].split('-',1)# http://stackoverflow.com/a/931095
    temp_id = int(revd_parts[0][::-1])
    temp_room = revd_parts[1][::-1]
    
    return (temp_room,temp_id)


def utf8_print(msg):
    print(str(msg),file=utf8stdout)
    
def log(data,room):
    global BYTES_RECV
    if room == "":
        filename = BASE_PATH+"\\MASTER\\play_log.txt"
        
    else:
        temp_room, temp_id = split_room_into_parts(room)
        filename = BASE_PATH+"\\MASTER\\Play\\"+temp_room+"\\"+room+".txt"
        BYTES_RECV = BYTES_RECV + len(data)
    # http://stackoverflow.com/a/12517490
    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))
    with open(filename, "ab") as f:
        f.write(bytes(data+"\n\r",'UTF-8'))

def recv_handler():
    try:
        while(1):
            if not RECV_QUEUE.empty():
                data = RECV_QUEUE.get()

                if data[0:1] == '>':
                    newline = data.index("\n")
                    msg = data[newline+1:]
                    room = data[1:newline]
                    
                    if msg[0:6] == "|init|": # Joined a new room
                        update_room(room)
                        log(msg,room)
                        ROOMS.append(room)
                    elif msg[0:8] == "|expire|" or msg[0:7] == "|deinit": # An already joined room no longer exists
                        remove_room(room)
                    elif msg[0:8] == "|noinit|": # Room doesn't exists
                        remove_room(room)
                    else:
                        log(msg,room)
                        
                    
                elif data[0:24] == "|queryresponse|roomlist|":
                    json_string = json.loads(data[24:])['rooms']
                    for j in json_string:
                        if j not in JOINED_ROOMS:
                            JOINED_ROOMS.append(j)
                            SEND_QUEUE.put("|/join "+j)
                else:
                    log("unknown: "+data,"")
            else:
                time.sleep(1)
    except:
        traceback.print_exc()
        
#Unicode was giving me problems.
def move_file(source,dest):
    if not os.path.exists(os.path.dirname(dest)):
        os.makedirs(os.path.dirname(dest))
    
    with open(source, "rb") as in_file:
        with open(dest, "wb") as out_file:
            out_file.write(in_file.read())

def getDate():
    global LAST_DATE
    
    stagingDir = 'C:\\Users\\carterj3\\Desktop\\Hadoop\\Staging\\'
    playDir = 'C:\\Users\\carterj3\\Desktop\\Hadoop\\Showdown\Log\\'
    
    newDate = datetime.datetime.now().strftime("%Y-%m-%d")
    if newDate != LAST_DATE: # Upload to server
        tarLoc = stagingDir+'\\'+LAST_DATE+'.tar'
        folderLoc = playDir+LAST_DATE
    
        TarAndSendToServer.tarFile(folderLoc,tarLoc)
        TarAndSendToServer.moveFile(tarLoc,'tmp/Showdown/Data/'+LAST_DATE+'.tar')
        LAST_DATE = newDate
    return datetime.datetime.now().strftime("%Y-%m-%d")
            
def remove_room(room):
    try:
        temp_room, temp_id = split_room_into_parts(room)
        date = getDate()
        
        filename = BASE_PATH+"\\MASTER\\Play\\"+temp_room+"\\"+room+".txt"
        new_filename = BASE_PATH+"\\MASTER\\Log\\"+date+"\\"+temp_room+"\\"+room+".txt"
        
        move_file(filename,new_filename)
    except:
        traceback.print_exc()
    try:
        ROOMS.remove(room)
    except:
        pass
        #traceback.print_exc()

def update_room(room):
    if VERBOSE:
        print("Joined ["+room+"]",file=utf8stdout)
    temp_time = time.time()
    no_battle = room.split('-',1)[1] # strip off battle-
    temp_room, temp_id = split_room_into_parts(no_battle)

def run():
    global temp_ou_id, temp_randombattle_id, OU, RANDOMBATTLE
    while(1):
        if SEND_QUEUE.empty():
            SEND_QUEUE.put("|/cmd roomlist")
            time.sleep(1*60)
        time.sleep(2)

run_thread = threading.Thread(target=run, args=())
run_thread.daemon = True
run_thread.start()

recv_handler_thread = threading.Thread(target=recv_handler, args=())
recv_handler_thread.daemon = True
recv_handler_thread.start()

def ws_recv(ws,bool_obj):
    #utf8_print("WS Recv")
    try:
        while(1):
            data = ws.recv()
            #utf8_print("Recv d: "+data)
            RECV_QUEUE.put(data)
            if bool_obj['dead']:
                return
    except:
        traceback.print_exc()
    bool_obj['dead'] = True
    log('Thread '+str(bool_obj['ID'])+' has died (recv)','')

def ws_send(ws,bool_obj):
    #utf8_print("WS Send")
    try:
        while(1):
            if not SEND_QUEUE.empty():
                data = SEND_QUEUE.get()
                #utf8_print("Sending d: "+data)
                ws.send(data)
                time.sleep(4)
                if bool_obj['dead']:
                    return
    except:
        traceback.print_exc()
    bool_obj['dead'] = True
    log('Thread '+str(bool_obj['ID'])+' has died (send)','')

def worker():
    global THREAD_ID
    while(1):
        log('Starting:: '+str(THREAD_ID),'')
        ws = create_connection(PS_URL)
        bo = {'dead':False,'ID':THREAD_ID}
        websocket_send = threading.Thread(target=ws_send, args=(ws,bo))
        websocket_recv = threading.Thread(target=ws_recv, args=(ws,bo))
        
        websocket_send.daemon = True
        websocket_recv.daemon = True
        
        websocket_send.start()
        websocket_recv.start()
        THREAD_ID = THREAD_ID + 1
        
        websocket_recv.join()
        websocket_send.join()
        
        time.sleep(5)
    
    #utf8_print("Worker stopped")
def spawn_thread():
    t = threading.Thread(target=worker, args=())
    t.daemon = True
    t.start()
    
    return t

while(1):
    getDate()
    cmd = input(">>:")
    if cmd == "list":
        # List connections
        print("There are "+str(len(CONNS))+" current connections")
        print(str(ROOMS))
    elif cmd == "stop":
        print("Joined rooms: "+str(ROOMS))
        log(str(ROOMS),'')
        break
    elif cmd == 'v':
        VERBOSE = not VERBOSE
    elif cmd == 'info':
        print(str(BYTES_RECV/(time.time() - START_TIME))+ "Bytes/Second")
    elif cmd == 'spawn':
        CONNS.append(spawn_thread())
    elif cmd == 'date':
        print(LAST_DATE)
# import logging
#logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.DEBUG)
import asyncio
from pyrogram import Client, filters
from dotenv import load_dotenv
import os
from io import BytesIO
from cryptography.fernet import Fernet
from collections import defaultdict
from cachetools import LRUCache
import gc
import traceback
import threading

from telegram_model_2 import TelegramFile

load_dotenv()

# FILE_MAX_SIZE_BYTES = int(2 * 1e9) # 2GB

FILE_MAX_CHUNKS=1907     #1 chunk = 1MiB
FILE_MAX_SIZE_BYTES=int(1907*1024*1024)

#extra chunks to download
EXTRA_CHUNKS=10

# Real LRU cache implementation very cool
CACHE_MAXSIZE = 5e9 # 5GB
def getsizeofelt(val):
    try:
        s = len(val)
        return s
    except:
        return 1

def progress_cb(sent_bytes, total):
    percentTotal = int(sent_bytes/total * 100)
    if percentTotal % 5 == 0:
        print(f"Progress: {percentTotal}%...")


class TelegramFileClient():
    def __init__(self, session_name, api_id, api_hash, channel_id):
        self.client = Client(session_name, api_id=api_id, api_hash=api_hash, no_updates =False,max_concurrent_transmissions =100)
        self.client.start()
        print(self.client.get_me())
        self.channel_entity = channel_id
        # key to use for encryption, if not set, not encrypted.
        self.encryption_key = os.getenv("ENCRYPTION_KEY")
        self.cached_files = LRUCache(CACHE_MAXSIZE, getsizeof=getsizeofelt)
        # self.cached_tele_messages = LRUCache(1e9/10, getsizeof=getsizeofelt)
        # self.fname_to_msgs = defaultdict(tuple)
        # self.ongoing_download_connections=0
        # self.will_be_available={}   #{msg_id:[this_offset_in_mb]}

        print("USING ENCRYPTION: ", self.encryption_key != None)

    

    #IMPLEMENTATION Not removed BYME
    def get_cached_file(self, fh, offset, length):
        # print(type(fh))
        # print(self.cached_files)
        # print(type(self.cached_files))
        #print(f"[BYME] CACHE REQUIRED fh {fh} offset:{offset}, length:{length}")
        #return None
        if fh in self.cached_files and self.cached_files[fh] != bytearray(b''):
            # print("CACHE HIT")
            return self.cached_files[fh]
        return None



    def delete_messages(self, ids):
        print("BYME Sorry wont delete msgs. preserving filesystem history")
        return
        return self.client.delete_messages(self.channel_entity, message_ids=ids)
    

    async def get_file(self, msg_ids, offset_in_bytes, length_in_bytes):
        print(f"\nrequesting offset {offset_in_bytes} length {length_in_bytes}")
        if msg_ids[0] not in self.cached_files:
            self.cached_files[msg_ids[0]]=TelegramFile(self.client, msg_ids)

        _= await self.cached_files[msg_ids[0]].get_file(offset_in_bytes,length_in_bytes)
        print(f"\nrequest fulfilled offset {offset_in_bytes} length {length_in_bytes}")
        return _


        







    async def upload_file(self, bytesio, fh, file_name=None, msg_ids=None):
        print(f"[BYME] UPLOAD FUNCTION fh {fh}, filename {file_name}")
        #testing by uploading at once
        # fname = f"{file_name}.txt" # convert everything to text. tgram is weird about some formats
        # chunkBytes = bytesio
        # chunkBytes.name=fname
        # bytesio.seek(0)
        # result = await self.client.send_document(self.channel_entity,
        #                                        bytesio,
        #                                        file_name=fname,
        #                                        progress=progress_cb)
    
        # return [result]
    

        # print(f"Type of bytesio {bytesio}")
        # invalidate cache as soon as we upload file
        if msg_ids and msg_ids[0] in self.cached_files:
            self.cached_files.pop(msg_ids[0])
            print("CLEANED UP ", gc.collect())

        print("me111mmemme")
        # file_bytes is bytesio obj
        file_bytes = bytesio.read()
        print("meaaammemme")
        if self.encryption_key != None:
            print("ENCRYPTING")
            f = Fernet(bytes(self.encryption_key, 'utf-8'))
            file_bytes = f.encrypt(file_bytes)

        chunks = []
        file_len = len(file_bytes)
        print("memmemme")
        if isinstance(file_len, float):
            print("UH OH FILEBYTES LENGTH IS FLOAT", file_len)
        if isinstance(FILE_MAX_SIZE_BYTES, float):
            print("UH OH FILE_MAX_SIZE_BYTES IS FLOAT", FILE_MAX_SIZE_BYTES)

        if file_len > FILE_MAX_SIZE_BYTES:
            # Calculate the number of chunks needed
            num_chunks = (len(file_bytes) + FILE_MAX_SIZE_BYTES) // FILE_MAX_SIZE_BYTES
            
            if isinstance(num_chunks, float):
                print("UH OH num_chunks IS FLOAT", num_chunks)

            # Split the file into chunks
            for i in range(num_chunks):
                start = i * FILE_MAX_SIZE_BYTES
                end = (i + 1) * FILE_MAX_SIZE_BYTES
                chunk = file_bytes[start:end]
                chunks.append(chunk)
        else:
            # File is within the size limit, no need to split
            chunks = [file_bytes]

        upload_results = []

        fname=file_name

        i = 0
        for c in chunks:
            print("hjnj")
            fname = f"{file_name}_part{i}.txt" # convert everything to text. tgram is weird about some formats
            chunkBytes = BytesIO(c)
            chunkBytes.name=fname
            print(f"Type of chunkbytes {type(chunkBytes)}")
            result = await self.client.send_document(self.channel_entity,
                                               chunkBytes,
                                               file_name=fname,
                                               progress=progress_cb)
            # result = self.client.send_file(self.channel_entity, f)
            upload_results.append(result)
            i += 1

        self.fname_to_msgs[file_name] = tuple([m.id for m in upload_results])
        print(f"CACHED FILE! NEW SIZE: {self.cached_files.currsize}; maxsize: {self.cached_files.maxsize}")
        return upload_results
    



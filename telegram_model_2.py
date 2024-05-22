import asyncio
import math
import time
import traceback

import logging
logging.basicConfig(level=logging.DEBUG)



class TelegramFile:
    def __init__(self, client, msg_ids):
        self.pyro_client=client
        self.msg_ids= msg_ids
        self.CHUNK_SIZE=1024*1024
        self.PART_MAX_CHUNKS=1907    #maximum chunks in a file part

        self.messages_list=None
        self.channel_entity=-1002017335161   #tele channel id

        self.total_file_size_in_bytes_var=None

        self.lock= asyncio.Lock()

        self.generator:TeleGenerator=None  
        
        
    

    def byte_index_to_global_chunk_index(self, byte_index):
        return byte_index//self.CHUNK_SIZE  #global offset in which this byte is present
    
    async def get_messages(self, force_api=False):
        async with self.lock:
            #todo implement force_api
            
            #messages already exist
            if self.messages_list:
                return self.messages_list
            
            #fetch messages from tele api
            else:
                print(f"[BYME][TELEGRAM API] get msg ids {self.msg_ids}")
                try:
                    self.messages_list = await self.pyro_client.get_messages(self.channel_entity, self.msg_ids)
                    return self.messages_list
                except Exception as e:
                    print(f"[Error] in fetching messages from telegram API. msg ids:{self.msg_ids}\n{traceback.format_exc()}")


    async def total_file_size_in_bytes(self):
        if self.total_file_size_in_bytes_var:
            return self.total_file_size_in_bytes_var
        
        #calculate file size
        messages = await self.get_messages()
        self.total_file_size_in_bytes_var = sum([msg.document.file_size for msg in messages])
        return self.total_file_size_in_bytes_var


    async def max_byte_index(self):
        total_file_size= await self.total_file_size_in_bytes()
        return total_file_size -1
    

    async def max_global_chunk_index(self):
        max_byte_index = await self.max_byte_index()
        return self.byte_index_to_global_chunk_index(max_byte_index)
    



    
    
    
    def multiple_chunks_to_required_bytes(self, chunks_bytearray, global_offset, global_endset, req_byte_offset, req_byte_endset) -> bytearray:
        byte_length_required= req_byte_endset-req_byte_offset +1

        byte_offset_within_first_chunk = req_byte_offset % self.CHUNK_SIZE

        return chunks_bytearray[byte_offset_within_first_chunk:byte_offset_within_first_chunk+byte_length_required]
        



    #entry point
    async def get_file(self, byte_offset, length_in_bytes):
        if not byte_offset:
            byte_offset=0
        
               
        if not length_in_bytes:
            byte_endset = await self.max_byte_index()
        else:
            byte_endset=byte_offset+length_in_bytes-1

        #check if byte_offset and byte_endset is within file range
        #byte_offset out of file
        if byte_offset> await self.max_byte_index():
            print("[INFO]Byte offset was out of file. returning empty")
            return bytearray()
        
        #byte_endset out of file
        if byte_endset > await self.max_byte_index():
            print("[INFO]Byte endset was out of file. changed to max possible endset")
            byte_endset= await self.max_byte_index()


        


        #convert to respective global chunk offset and endset
        global_chunk_offset=self.byte_index_to_global_chunk_index(byte_offset)
        global_chunk_endset=self.byte_index_to_global_chunk_index(byte_endset)

        #fetch global_chunk_offset -> global_chunk_endset
        chunks_bytearray = await self.get_chunks_across_multiple_files(global_chunk_offset, global_chunk_endset)


        #convert chunks_bytearray to required_bytes_bytearray and return
        return self.multiple_chunks_to_required_bytes(chunks_bytearray, global_chunk_offset, global_chunk_endset, byte_offset, byte_endset)
    
    def global_index_to_this(self, global_chunk_index):    #for chunks(mb)
            file_index=global_chunk_index//self.PART_MAX_CHUNKS
            this_chunk_index= global_chunk_index % self.PART_MAX_CHUNKS
            return file_index, this_chunk_index




    async def get_chunks_across_multiple_files(self, global_chunk_offset, global_chunk_endset) -> bytearray:
        #get (chunk_offset, chunk_endset) for every file part

        first_part_index, first_part_chunk_offset = self.global_index_to_this(global_chunk_offset)
        last_part_index, last_part_chunk_endset = self.global_index_to_this(global_chunk_endset)

        
        asyncio_tasks=[]
        for file_index in range(first_part_index, last_part_index + 1):
            msg_id=self.msg_ids[file_index]

            if file_index == first_part_index:
                this_part_chunk_offset = first_part_chunk_offset
            else:
                this_part_chunk_offset = 0

            if file_index == last_part_index:
                this_part_chunk_endset = last_part_chunk_endset
            else:
                this_part_chunk_endset = self.PART_MAX_CHUNKS -1

            #append get this file part to asyncio_tasks
            asyncio_tasks.append(asyncio.create_task(self.get_chunks_from_one_part(file_index, this_part_chunk_offset,this_part_chunk_endset)))
            

        
        # Await all part tasks to be complete
        parts_results = await asyncio.gather(*asyncio_tasks)

        # Combine all parts into the result_buffer
        result_buffer = bytearray()
        for buffer in parts_results:
            result_buffer.extend(buffer)

        return result_buffer


    def this_index_to_global(self, file_index, part_chunk_index):
        global_index= file_index * self.PART_MAX_CHUNKS +part_chunk_index
        return global_index

    async def get_chunks_from_one_part(self, file_index, chunk_offset, chunk_endset):
        global_chunk_offset=self.this_index_to_global(file_index, chunk_offset)
        global_chunk_endset=self.this_index_to_global(file_index, chunk_endset)

        result_bytearray=bytearray()


        messages = await self.get_messages()
        async with self.lock:
            if self.generator == None:
                #create new generator
                self.generator = TeleGenerator(self, file_index, global_chunk_offset, messages)

        async with self.lock:
            return await self.generator.can_generate(global_chunk_offset, global_chunk_endset)

        



import asyncio
import math
import time
import traceback
import logging

logging.basicConfig(level=logging.INFO)

class TeleGenerator:
    def __init__(self, file, file_index, global_chunk_offset, messages) -> None:
        self.file = file
        self.file_index = file_index
        self.EXTRA_CHUNKS = 10
        self.max = None
        self.lock = asyncio.Lock()
        self.background_task = None
        self.messages=messages
        self.generator= None
        self.current_running_generator_index=None  #this index is not yet generated
        self.current_running_generator_offset =None
        self.cached_data={}
        logging.info(f"Generator object created for {global_chunk_offset}")

    async def can_generate(self, global_chunk_offset, global_chunk_endset):
        # Step 1: Iterate for all offsets
        # Step 2: Not in cache? Generate till required
        result_bytearray = bytearray()
        for offset in range(global_chunk_offset, global_chunk_endset + 1):
            if offset in self.cached_data and self.cached_data[offset] != bytearray():
                result_bytearray.extend(self.cached_data[offset])
            else:
                # Generate
                chunk = await self.generate_chunks(offset)
                result_bytearray.extend(chunk)

        return result_bytearray

    async def get_generator(self, global_offset):
        if self.generator ==None:
            #no prev generator. create new
            self.generator = self._generate_chunks(global_offset)
        
        else:
            #check if prev generator can do the job
            if (global_offset>=self.current_running_generator_offset) and (global_offset - self.current_running_generator_index < 10):
                #yes use this
                pass
            else:
                #delete old gen, create new
                _ =  self.generator
                del _
                self.generator = self._generate_chunks(global_offset)

        return self.generator



    # Not manual use
    async def generate_chunks(self, global_chunk_index):  # Returns one chunk
        if self.background_task is not None:
            # Stop background downloads
            s = time.time()
            self.background_task.cancel()
            try:
                await self.background_task
            except asyncio.CancelledError:
                self.background_task = None

            logging.info(f"Background task was cancelled in {int(time.time() - s)}s")

        # Find generator or create
        gen = await self.get_generator(global_chunk_index)

        # async with self.lock:
        while global_chunk_index not in self.cached_data:
            logging.info("do anext")
            try:
                async with self.lock:
                    await anext(gen)
            except StopAsyncIteration:
                logging.info("Generator exhausted")
                _ =  self.generator
                del _
                self.generator =None
                break
            except Exception as e:
                logging.error(traceback.format_exc())
            logging.info("anext done")


        return self.cached_data[global_chunk_index]

    # NOT FOR MANUAL USAGE
    async def _generate_chunks(self, start_offset):
        logging.info("self._generate_chunks()")
        self.current_running_generator_offset= start_offset

        current_global_chunk_index = start_offset
        

        
        print("here3")
        # Convert global start offset to this part offset for stream media
        _, this_part_offset = self.file.global_index_to_this(start_offset)

        print("here4")

        logging.info(f"Using telegram api to stream from {this_part_offset}")
        async for chunk in self.file.pyro_client.stream_media(self.messages[self.file_index], offset=this_part_offset):
            logging.info(f"Got offset {current_global_chunk_index}")
            self.cached_data[current_global_chunk_index] = bytearray(chunk)
            current_global_chunk_index += 1
            self.current_running_generator_index = current_global_chunk_index
            yield

        
        
    async def max_global_offset_for_this_file(self):
        if self.max is not None:
            return self.max
        
        self.total_file_size_bytes = self.messages[self.file_index].document.file_size
        total_chunks = self.file_index * self.file.PART_MAX_CHUNKS + math.ceil(self.total_file_size_bytes / self.file.CHUNK_SIZE)
        self.max = total_chunks - 1
        return self.max

    async def run_background_task(self, global_chunk_endset):
        logging.info(f"bg created offset {global_chunk_endset}")
        if self.completed:
            return

        max_offset = await self.max_global_offset_for_this_file()
        if global_chunk_endset > max_offset:
            global_chunk_endset = max_offset

        # Generate till max_offset
        try:
            while self.current_global_chunk_index <= global_chunk_endset:
                async for _ in self.generate_next():
                    break
        except asyncio.CancelledError:
            logging.info("Task was cancelled")
            raise

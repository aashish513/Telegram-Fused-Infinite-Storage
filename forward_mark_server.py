from flask import Flask, Response, request
app = Flask(__name__)
import os
from pyrogram import Client,filters 
from pyrogram.types import (InlineKeyboardMarkup,InlineKeyboardButton)
import time
import re
import threading
import urllib.parse

from dotenv import load_dotenv
load_dotenv()
bot= Client("forward_mark_bot_session", api_id=os.getenv("APP_ID"), api_hash=os.getenv("APP_HASH"))
available_files={} #{23432:[1,2,3]}
required_files=[] #[[msg_obj,mb],[]..]
will_be_available={}  #{23432:[1,2,3]}
dirs=[]

@app.route("/<chat_id>/<msg_id>/<file_name_in_url>")
def serve(chat_id,msg_id,file_name_in_url):
  range_header = request.headers.get('Range', None)
  try:
    msg=bot.get_messages(int(chat_id), int(msg_id))
    print('resolved message id'+str(msg_id))
    media=getattr(msg, msg.media.value)
    file_name=getattr(media,'file_name',media.file_unique_id)
    file_name=urllib.parse.unquote(file_name)
  except:
    return "ok"
  if file_name!=file_name_in_url:
    return "ok"
  file_size=getattr(media,'file_size') 
  mimetype=getattr(media,'mime_type','sample_mime')
  #size_mb=file_size/(1024*1024)
  mb,byte=bytes_in_mb(file_size)
  if byte==0:
    last_chunk=mb-1
  else:
    last_chunk=mb
  if msg.id not in available_files:
    available_files[msg.id]=[]
  if msg.id not in will_be_available:
    will_be_available[msg.id]=[]
  def generate(start=None,end=None):
    if start==None:
      mb=0
      while True:
        #print("fetching mb:"+str(mb))
        if mb==last_chunk:
          #print("sending last mb:"+str(mb))
          yield get_file(msg,mb)
        elif mb>last_chunk:
          print("no more chunks.. end connection now ")
          return None
        else:
          #print("sending mb:"+str(mb))
          yield get_file(msg,mb)
        mb=mb+1
    else:
      first_mb, dlt_start=bytes_in_mb(start)   #dlt dltstart no of bytes from first_mb_chunk
      last_mb, dlt_last=bytes_in_mb(end)    #keep (dlt_last+1) no of bytes from last_mb_chunk
      mb=first_mb
      while True:
        #print("fetching mb:"+str(mb))
        if mb==first_mb:
          s=bytes(bytearray(get_file(msg,mb))[dlt_start:])
          if first_mb==last_mb:
            yield bytes(bytearray(s)[:dlt_last+1])
            print("first_mb and last_mb same...ending connection")
            return None
          else:
            yield s
        elif mb==last_mb:
          print("sending last partial content. last chunk of file is "+str(last_chunk)+"...mb:"+str(mb))
          yield bytes(bytearray(get_file(msg,mb))[:dlt_last+1])
        elif mb>last_mb:
          print("no more partial chunks.. end connection now ")
          return None
        else:
          #print("sending partial mb:"+str(mb))
          yield get_file(msg,mb)
        mb=mb+1
  if range_header ==None:
    resp= Response(generate(), mimetype=mimetype)
    resp.headers['Content-Disposition'] = f'attachment; filename="{file_name}"'
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Content-Length'] = file_size
    resp.headers['Accept-Ranges'] =  "bytes"
    return resp
  else:
    byte1, byte2 = 0, file_size-1
    m = re.search('(\d+)-(\d*)', range_header)
    g = m.groups()
    if g[0]: byte1 = int(g[0])
    if g[1]: byte2 = int(g[1])
    #length = file_size - byte1
    #if byte2 is not None:
    #    length = byte2 - byte1
    print(range_header)
    print(byte1)
    #print(byte1/(1024*1024))
    print(byte2)
    #print(length)
    print(file_size)
    resp = Response(generate(byte1,byte2), 206, mimetype=mimetype, direct_passthrough=True)
    resp.headers['Content-Range']= f'bytes {byte1}-{byte2}/{file_size}'
    resp.headers['Content-Disposition'] = f'attachment; filename="{file_name}"'
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Accept-Ranges'] =  "bytes"
    return resp



def bytes_in_mb(bytes):  #outpt: mb, bytes
  q = bytes//(1024*1024)
  r = bytes%(1024*1024)
  return (q,r)




def down_req_files(): #running in thread
  global required_files
  global will_be_available 
  print("running this down_req_files")
  while True:
    for i in required_files:
      if i[1] in will_be_available[i[0].id]:
        continue
      print("starting thread for mb:"+str(i[1]))
      threading.Thread(target=download,args=(i[0],i[1],)).start()
      required_files.remove(i)
      will_be_available[i[0].id].append(i[1])
    time.sleep(0.5)

    
def download(msg, mb):
  global will_be_available
  global available_files
  global dirs
  if msg.id not in dirs:
      try:
        os.mkdir(f'/tmp/{msg.id}')
      except:
        pass
      dirs.append(msg.id)
  stop=mb+200
  for chunk in bot.stream_media(msg, offset=mb):
    if mb in available_files[msg.id]:
      return
    with open(f'/tmp/{msg.id}/{mb}','wb') as f:
      f.write(chunk) 
    print(f"downloaded {msg.id}:{mb}")
    try:
      if mb==stop:
        print("stopped at "+str(stop))
        return
      else:
        try:
          will_be_available[msg.id].append(mb+1)
        except:
          pass
    finally:
      try:
        will_be_available[msg.id].remove(mb)
        available_files[msg.id].append(mb)
      except:
        pass
    mb=mb+1


def get_file(msg,mb): #mb=int
  global will_be_available
  global required_files
  if mb in available_files[msg.id]:
    return open(f'/tmp/{msg.id}/{mb}','rb').read()
  elif mb in will_be_available[msg.id]:
    time.sleep(0.2)
    a=0
    while True:
      if mb in available_files[msg.id]:
        return open(f'/tmp/{msg.id}/{mb}','rb').read()
      time.sleep(0.05)
      #print("slept here for mb: "+str(mb))
      a=a+1
      if a>200:
        will_be_available[msg.id].remove(mb)
        a=0
        required_files.append([msg,mb])
  else:
    required_files.append([msg,mb])
    time.sleep(1.5)
    while True:
      if mb in available_files[msg.id]:
        return open(f'/tmp/{msg.id}/{mb}','rb').read()
      time.sleep(0.3)
      #print("slept here 332")







@app.route('/')
def default():
  return "ok"




@bot.on_message(filters.private)
def bot_msg(client, update):
  print(update)
  media_type=str(update.media).split('.')[-1]
  if media_type in ['AUDIO','DOCUMENT','PHOTO','VIDEO','VOICE','VIDEO_NOTE']:
    media=getattr(update, update.media.value)
    file_name=getattr(media,'file_name',media.file_unique_id)
    print(file_name)
    file_name=urllib.parse.quote(file_name)
    url=f"https://forwardmark.aashishsinhasin.repl.co/{update.chat.id}/{update.id}/{file_name}"
    update.reply("Link to Download: "+url,
            reply_markup=InlineKeyboardMarkup(
                [                    [InlineKeyboardButton("Download",url=url),]
                ]
            ))
  else:
    update.reply("Send an image or file to get a link to download it")
  
'''@bot.on_callback_query()
def answer(client, callback_query):
  res=prepare(callback_query.data.split(',')[0],callback_query.data.split(',')[1])
  callback_query.answer(res,show_alert=True)
'''



print("starting bot")
def server():
  time.sleep(2)
  app.run(host='0.0.0.0', port=8080)
  


threading.Thread(target=server).start()
threading.Thread(target=down_req_files).start()


print("bot is gonna start now")
bot.run()
print("it started")




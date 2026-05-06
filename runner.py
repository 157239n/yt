from common import *

import bs4
def getUrls(htm):
    ht = bs4.BeautifulSoup(htm, "html.parser"); urls = []
    for a in ht.select("ytd-rich-item-renderer"):
        try: urls.append("https://www.youtube.com" + a.select_one("h3 > a").attrs["href"])
        except: pass
    return urls
def getUrlsFromElems(es):
    urls = []
    for i, e in enumerate(es):
        warnings.warn(f"getUrlsFromElems: {i}")
        a = bs4.BeautifulSoup(e.innerHTML, "html.parser")
        try: urls.append("https://www.youtube.com" + a.select_one("h3 > a").attrs["href"])
        except: pass
    warnings.warn(f"finished, og es: {len(es)}, urls: {len(urls)}"); return urls@k1.cron(delay=10)

def getYtVid(url): return db["videos"].lookup(vidId=getYtVidId(url))
def ingestUrls(urls, guardRes, channel):
    for url in urls:
        try: api_vid_new({"url": url}, guardRes)
        except web._ShortCircuit as e: pass
        vid = getYtVid(url)
        if vid: vid.channelId = channel.id

@app.route("/api/channel/<int:channelId>/fullScan", guard=tokenGuard)
def api_channel_fullScan(channelId, guardRes):
    channel = db["channels"][channelId]
    with zircon.newBrowser() as b:
        b.pickExtFromGroup("site"); b.goto(f"https://www.youtube.com/{channel.handle}/videos"); time.sleep(1)
        main = b.querySelector("div:has(ytd-rich-item-renderer)"); oldHeight = 0
        for i in range(20):
            warnings.warn(f"fullScan {i}, {oldHeight}"); newHeight = main.clientHeight
            if oldHeight == newHeight: break
            k1.resolve(b._sendExt({"cmd": "scrollAt", "x": 500, "y": 500, "deltaY": 100000})); time.sleep(1)
        ingestUrls(getUrlsFromElems(b.querySelectorAll("ytd-rich-item-renderer")), guardRes, channel)
    return "ok"
def partialScan(b, handle):
    b.goto(f"https://www.youtube.com/{handle}/videos"); time.sleep(1)
    main = b.querySelector("div:has(ytd-rich-item-renderer)"); oldHeight = 0; urls = []
    for i in range(20):
        newUrls = getUrls(main.innerHTML)
        for url in newUrls:
            if getYtVid(url): return urls
            urls.append(url)
        newHeight = main.clientHeight
        if oldHeight == newHeight: break
        k1.resolve(b._sendExt({"cmd": "scrollAt", "x": 500, "y": 500, "deltaY": 100000}))
    return urls
@app.route("/api/channel/<int:channelId>/partialScan", guard=tokenGuard)
def api_channel_partialScan(channelId, guardRes):
    channel = db["channels"][channelId]
    with zircon.newBrowser() as b: b.pickExtFromGroup("yttri"); ingestUrls(partialScan(b, channel.handle), guardRes, channel)
    return "ok"

def channel_fullscan_loop():
    for channel in db["channels"].select("where fullscanErr is null limit 1"):
        try: api_channel_fullScan(channel.id, {"userId": 1}); channel.fullscanErr = ""; channel.fullscanTime = int(time.time())
        except Exception as e: channel.fullscanErr = f"{type(e)} | {e}\n{traceback.format_exc()}"

@k1.cron(delay=60)
def archivedSyncLoop():
    chatIds = db.query("select chatId from access where (archived is null or archived = 0) and chatId is not null") | cut(0) | aS(list)
    for chatId, archivedTime in sendAiServer(1, {"cmd": "syncArchived", "chatIds": chatIds}).json():
        access = db["access"].lookup(chatId=chatId); access.archived = 1; access.archivedTime = archivedTime

providers = {"yt": "https://www.youtube.com/watch?v=", "dailymotion": "https://www.dailymotion.com/video/"}

@k1.cron(delay=10)
def titleLoop():
    for vid in db["videos"].select("where title is null"):
        if vid.provider in providers:
            res = None | cmd(f'{yt_dlp} --cookies cookies.txt --print "%(title)s" {providers[vid.provider]}{vid.vidId}', mode=0) | apply(join("\n")) | deref()
            vid.title = res[0] if res[0].strip() else res | join("\n")
        else: vid.title = f"Unknown provider {vid.provider}"; continue

import shlex
@k1.cron(delay=60)
def vidLoop(): # auto detects videos that need to be taken care of
    for vid in db["videos"].select("where vidErr is null and title is not null order by id desc limit 1"):
        print(f"vid: {vid.id}, provider: {vid.provider}")
        if vid.provider in providers:
            res = None | cmd(f'{yt_dlp} --cookies cookies.txt -o "tmpVids/new.%(ext)s" {providers[vid.provider]}{vid.vidId}', mode=0) | deref()
            fns = "tmpVids" | ls() | grep("new") | deref()
            if len(fns) == 0: vid.vidErr = f"Tried to download, no new.mp4 or new.webm or others found in tmpVids: {res}"; continue
            None | cmd(f"mv {fns[0]} vids/{vid.vidId}") | ignore(); vid.vidErr = ""; vid.vidTime = int(time.time())
        elif vid.provider == "fs":
            None | cmd(f"cp {shlex.quote(vid.url)} vids/{vid.vidId}") | ignore()
            vid.vidErr = "" if os.path.exists(f"vids/{vid.vidId}") else "Tried to copy the file over but can't for some reason"
        else: vid.vidErr = f"Unknown provider '{vid.provider}'"

@k1.cron(delay=10)
def magicLoop():
    for vid in db["videos"].select("where vidErr='' and mime is null"):
        print(f"magic: {vid.id}")
        with open(f"vids/{vid.vidId}", "rb") as f: vid.mime = magic.Magic(mime=True).from_buffer(f.read())

@k1.cron(delay=10)
def matroskaLoop(): # detects videos that are of matroska format, and reformat it to webm so browsers can actually play it
    return # mostly discarded anyway, so let's not waste cpu cycles on this
    for vid in db["videos"].select("where mime = 'video/x-matroska' limit 1"):
        print(f"matroska: {vid.id}")
        res = None | cmd(f"ffmpeg -i vids/{vid.vidId} -c:v libvpx -crf 10 -b:v 2M -c:a libvorbis tmpVids/output.webm", mode=0) | deref()
        fns = "tmpVids" | ls() | grep("output.webm") | deref()
        if len(fns) == 0: vid.mime = "video/x-matroska-error"; print(f"matroska error: {res}"); continue
        None | cmd(f"mv {fns[0]} vids/{vid.vidId}") | ignore(); vid.mime = "video/webm"

# @k1.cron(delay=10)
def zeroLoop(): # detects videos where vidErr is '' (success), but video file does not exist
    for vid in db["videos"].select("where vidErr=''"):
        if not os.path.exists(f"vids/{vid.vidId}"): vid.vidErr = None

@k1.cron(delay=10)
def durationLoop():
    for vid in db["videos"].select("where vidErr='' and duration is null"):
        print(f"duration: {vid.id}")
        vid.duration = None | cmd(f"ffprobe -v error -show_entries format=duration -of default=nw=1 -i vids/{vid.vidId}") | join("") | op().strip().replace("duration=", "") | aS(float)

from faster_whisper import WhisperModel, BatchedInferencePipeline; model = BatchedInferencePipeline(model=WhisperModel("large-v3-turbo", device="cuda", compute_type="int8_float16"))
# gpu: device="cuda", compute_type="int8_float16"
# cpu: device="cpu", compute_type="int8", cpu_threads=16
def seconds_to_vtt_timestamp(seconds: float) -> str:
    total_milliseconds = int(round(seconds * 1000)); hours = total_milliseconds // 3_600_000
    minutes = (total_milliseconds % 3_600_000) // 60_000; secs = (total_milliseconds % 60_000) // 1000
    milliseconds = total_milliseconds % 1000; return f"{hours:02}:{minutes:02}:{secs:02}.{milliseconds:03}"
def getVtt(segments): return "WEBVTT\n\n" + (segments | apply(lambda segment: f"{seconds_to_vtt_timestamp(segment.start)} --> {seconds_to_vtt_timestamp(segment.end)}\n{segment.text.strip()}\n\n") | join(""))

@k1.cron(delay=10)
def transLoop():
    for vid in db["videos"].select("where vidErr = '' and transErr is null limit 1"):
        print(f"transcribe: {vid.id}")
        try: vid.trans = getVtt(model.transcribe(f"vids/{vid.vidId}", beam_size=1, batch_size=8)[0]); vid.transErr = ""
        except Exception as e: vid.transErr = f"Tried to transcribe, encountered error: {type(e)} | {e} | {traceback.format_exc()}"

@k1.cron(delay=1)
def summarizeLoop():
    # if len(db.query("select id from videos where vidErr = '' and transErr is null")) > 0: print("skipping summarize loop"); return # when there's stuff to transcribe, it consumes the gpu, which means text generation is going to be slow, so pause summarization if there's transcription going on
    for accessId, vidId in db.query("select a.id, v.id from videos v join access a on v.id = a.vidId where v.transErr = '' and v.duration is not null and a.chatId is null order by a.id limit 1"):
        access = db["access"][accessId]
        try:
            vid = db["videos"][vidId]; user = db["users"][access.userId]; print(f"summarize: {vid.id}")
            res = sendAiServer(user.id, {"cmd": "scheduleNewChat", "prompt": f"Please fetch the transcript of youtube video with id '{vid.vidId}' (title '{vid.title}', duration {vid.duration}) and summarize 2 times, once for a 1-2 paragraph  overview, and once in detail. Transcript might have small spelling errors (but not core facts), correct it if necessary"})
            access.chatId = int(res.text.strip())
        except Exception as e: access.chatId = f"error: {res.text.strip()}"

@k1.cron(delay=10)
def cleanLoop():
    now = int(time.time())
    for vid in db["videos"].select(f"where vidErr = '' and transErr = '' and createdTime < {now - 86400} and retain = 0 and cleaned = 0 limit 1"):
        None | cmd(f'rm vids/{vid.vidId}') | ignore(); vid.cleaned = 1


from common import *

__all__ = []

import secrets, string
def getYtVidId(url): vidId = url.split("/watch")[-1].strip("?").split("&")[0]; return parse_qs(urlparse(url).query).get("v", [None])[0]
@app.route("/api/vid/new", methods=["POST"], guard=tokenGuard)
def api_vid_new(js, guardRes):
    url = js["url"]; provider = None; userId = guardRes["userId"]; vidId = None
    if url.startswith("https://www.youtube.com/watch"):       provider = "yt"; vidId = getYtVidId(url)
    elif url.startswith("https://www.dailymotion.com/video"): provider = "dailymotion"; vidId = url.split("/video/")[1].split("/")[0].split("?")[0].split("&")[0].strip()
    elif url.startswith("fs:"):
        fullFn = url.split("fs:")[1]; fn = fullFn.split("/")[-1]; vidId = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(12)); provider = "fs"
        db["videos"].insert(url=fullFn, vidId=vidId, title=fn, vidErr=None, trans="", transErr=None, createdTime=int(time.time()), provider="fs", retain=0, cleaned=0)
    if vidId is None: web.toast_error("Can't extract vidId")
    if provider is None: web.toast_error("Don't know what service (youtube, dailymotion, etc) this url belongs to")
    vid = db["videos"].lookup(vidId=vidId, provider=provider)
    if vid:
        hasAccess = db["access"].lookup(vidId=vid.id, userId=userId)
        if hasAccess: web.toast_error("Video added before!")
        else: db["access"].insert(vidId=vid.id, userId=userId, chatId=None, archived=0); return "ok"
    vid = db["videos"].insert(url=url, vidId=vidId, title=None, vidErr=None, trans="", transErr=None, createdTime=int(time.time()), provider=provider, retain=0, cleaned=0)
    db["access"].insert(vidId=vid.id, userId=userId, chatId=None, archived=0); return "ok"

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
        print(f"\rgetUrlsFromElems: {i}   ", end="")
        a = bs4.BeautifulSoup(e.innerHTML, "html.parser")
        try: urls.append("https://www.youtube.com" + a.select_one("h3 > a").attrs["href"])
        except: pass
    print(f"finished, og es: {len(es)}, urls: {len(urls)}"); return urls

def getYtVid(url): return db["videos"].lookup(vidId=getYtVidId(url))
def ingestUrls(urls, channel): # ingest urls for all users that subscribes to the channel
    print("begin ingestUrls")
    userIds = db.query("select userId from subs where channelId = ?", channel.id) | cut(0) | aS(list)
    for i, url in enumerate(urls):
        print(f"\ringestUrls: {i}   ", end="")
        for userId in userIds:
            try: api_vid_new({"url": url}, {"userId": userId})
            except web._ShortCircuit as e: pass
        vid = getYtVid(url)
        if vid: vid.channelId = channel.id
    print("finished ingestUrls")

@app.route("/api/channel/<int:channelId>/fullScan", guard=tokenGuard)
@app.route("/api/channel/<int:channelId>/fullScan/<int:nScrolls>", guard=tokenGuard)
def api_channel_fullScan(channelId, nScrolls=20):
    channel = db["channels"][channelId]; print(f"fullScan init, channel: {channelId}")
    with zircon.newBrowser() as b:
        b.pickExtFromGroup("yt"); b.goto(f"https://www.youtube.com/{channel.handle}/videos"); time.sleep(1)
        main = b.querySelector("div:has(ytd-rich-item-renderer)"); oldHeight = 0
        for i in range(nScrolls):
            print(f"fullScan {i}, {oldHeight}"); newHeight = main.clientHeight
            if oldHeight == newHeight: break
            k1.resolve(b._sendExt({"cmd": "scrollAt", "x": 500, "y": 500, "deltaY": 100000})); time.sleep(1)
        ingestUrls(getUrlsFromElems(b.querySelectorAll("ytd-rich-item-renderer")), channel)
    return "ok"

@k1.cron(delay=1)
def channel_partscan_loop():
    for channel in db["channels"].select(f"where partscanTime < {int(time.time()) - 86400} limit 1"):
        try: api_channel_fullScan(channel.id, 0)
        except Exception as e: print(f"{type(e)} | {e}\n{traceback.format_exc()}")
        channel.partscanTime = int(time.time())

@k1.cron(delay=1)
def channel_fullscan_loop():
    print("fullscan loop")
    for channel in db["channels"].select("where fullscanErr is null limit 1"):
        try: api_channel_fullScan(channel.id, 20); channel.fullscanErr = ""; channel.fullscanTime = int(time.time())
        except Exception as e: channel.fullscanErr = f"{type(e)} | {e}\n{traceback.format_exc()}"

@k1.cron(delay=60)
def archivedSyncLoop():
    chatIds = db.query("select chatId from access where (archived is null or archived = 0) and chatId is not null and typeof(chatId) = 'integer'") | cut(0) | aS(list)
    for chatId, archivedTime in sendAiServer(1, {"cmd": "syncArchived", "chatIds": chatIds}).json():
        access = db["access"].lookup(chatId=chatId); access.archived = 1; access.archivedTime = archivedTime

providers = {"yt": "https://www.youtube.com/watch?v=", "dailymotion": "https://www.dailymotion.com/video/"}

@k1.cron(delay=1)
def titleLoop():
    for vid in db["videos"].select("where vidId = '' and title is null limit 1"):
        if vid.provider in providers:
            res = None | cmd(f'{yt_dlp} --cookies cookies.txt --print "%(title)s" {providers[vid.provider]}{vid.vidId}', mode=0) | apply(join("\n")) | deref()
            vid.title = res[0] if res[0].strip() else res | join("\n")
        else: vid.title = f"Unknown provider {vid.provider}"; continue

import shlex
@k1.cron(delay=60)
def vidLoop(): # auto detects videos that need to be taken care of
    common = "vidErr is null and title is not null"; channelIds = db.query(f"select distinct channelId from videos where {common} order by channelId") | cut(0) | aS(list) # grab unique channels that has undownloaded videos
    if len(channelIds) == 0: return
    if channelIds[0] is None: vidId = db.query(f"select id from videos where {common} and channelId is null order by id desc limit 1")[0][0] # looks for videos not in any channel first
    else: vidId = db.query(f"select id from videos where {common} and channelId = ? order by id desc limit 1", channelIds | randomize(None) | item())[0][0] # then pick a random channel and do its latest video first
    vid = db["videos"][vidId]; print(f"vid: {vid.id}, provider: {vid.provider}")
    if vid.provider in providers:
        res = None | cmd(f'{yt_dlp} --cookies cookies.txt -o "tmpVids/new.%(ext)s" {providers[vid.provider]}{vid.vidId}', mode=0) | deref()
        fns = "tmpVids" | ls() | grep("new") | deref()
        if len(fns) == 0: vid.vidErr = f"Tried to download, no new.mp4 or new.webm or others found in tmpVids: {res}"; return
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

@k1.cron(delay=10)
def durationLoop():
    for vid in db["videos"].select("where vidErr='' and duration is null"):
        print(f"duration: {vid.id}")
        try: vid.duration = None | cmd(f"ffprobe -v error -show_entries format=duration -of default=nw=1 -i vids/{vid.vidId}") | join("") | op().strip().replace("duration=", "") | aS(float)
        except Exception as e: vid.duration = f"{type(e)} | {e}\n{traceback.format_exc()}"

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
    for accessId, vidId in db.query("select a.id, v.id from videos v join access a on v.id = a.vidId where v.transErr = '' and v.duration is not null and typeof(v.duration) = 'real' and a.chatId is null order by a.id limit 1"):
        access = db["access"][accessId]
        try:
            vid = db["videos"][vidId]; user = db["users"][access.userId]; channel = db["channels"][vid.channelId]; print(f"summarize: {vid.id}"); channelS = f", from channel '{channel.handle}'" if channel else ""
            res = sendAiServer(user.id, {"cmd": "scheduleNewChat", "prompt": f"Please fetch the transcript of youtube video with id '{vid.vidId}' (title '{vid.title}', duration {vid.duration}s{channelS}) and summarize 2 times, once for a 1-2 paragraph  overview, and once in detail. Transcript might have small spelling errors (but not core facts), correct it if necessary"})
            access.chatId = int(res.text.strip())
        except Exception as e: access.chatId = f"error: {res.text.strip()}"

@k1.cron(delay=10)
def cleanLoop():
    now = int(time.time())
    for vid in db["videos"].select(f"where vidErr = '' and transErr = '' and vidTime < {now - 86400} and retain = 0 and cleaned = 0 limit 1"):
        None | cmd(f'rm vids/{vid.vidId}') | ignore(); vid.cleaned = 1


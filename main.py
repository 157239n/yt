from k1lib.imports import *
import magic
from urllib.parse import urlparse, parse_qs
from schemaParser import *

settings.timezone = "Asia/Hanoi"
yt_dlp = "/home/kelvin/envs/torch/bin/yt-dlp --js-runtimes 'deno:/home/kelvin/.deno/bin/deno' "
aiServer = "https://ai.aigu.vn"
ytServer = "https://yt.aigu.vn"

db = sql("dbs/main.db", mode="lite", manage=True)["default"]
db.query("""CREATE TABLE IF NOT EXISTS videos (
    id          INTEGER primary key autoincrement,
    url         TEXT,    -- full url of the youtube video
    vidId       TEXT,    -- youtube's video id string, like mDMQC0PtvYM
    title       TEXT,    -- title of the video, may be taken from zircon
    vidErr      TEXT,    -- if download not successful, contains the error and traceback. If successful, an empty string, if not executed, null
    trans       TEXT,    -- raw transcript of the video in webvtt format
    transErr    TEXT,    -- if transcription not successful, contains the error and traceback. If successful, an empty string, if not executed, null
    createdTime INTEGER, -- unix time
    mime        TEXT,
    duration    REAL,    -- duration of the video in seconds
    provider    TEXT,    -- what provider this video belongs to. yt/dailymotion
    retain      BOOL,    -- if True, retains the video, else deletes it to save space
    cleaned     BOOL,    -- whether this video has been removed to save space?
    channelId   INTEGER, -- what youtube channel does this video belong to?
    vidTime     INTEGER  -- what time does the vid finishes downloading?
);""")
db.query("CREATE INDEX IF NOT EXISTS videos_vidId ON videos (vidId);")
db.query("CREATE INDEX IF NOT EXISTS videos_channelId ON videos (channelId);")
db.query("""CREATE TABLE IF NOT EXISTS users ( -- this is just to keep track of what user has been initialized
    id          INTEGER primary key, -- no autoincrement because ground truth is on ai server's db
    scheduleId  INTEGER              -- scheduleId just for this youtube app
);""")
db.query("""CREATE TABLE IF NOT EXISTS access ( -- track what user has access to what videos
    id           INTEGER primary key, -- no autoincrement because ground truth is on ai server's db
    vidId        INTEGER,
    userId       INTEGER,
    chatId       INTEGER, -- chatId for this particular video summary. Can be a string for error message
    archived     BOOL,    -- this and one below synced from ai server, to do a bunch of calcs and whatnot
    archivedTime INTEGER
);""")
db.query("CREATE INDEX IF NOT EXISTS access_vidId ON access (vidId);")
db.query("CREATE INDEX IF NOT EXISTS access_userId ON access (userId);")

db.query("""CREATE TABLE IF NOT EXISTS channels ( -- tracks youtube channels
    id           INTEGER primary key autoincrement,
    provider     TEXT,   -- only yt for now
    handle       TEXT,   -- channel names, starts with @
    name         TEXT,   -- channel display name
    fullscanErr  TEXT,   -- if successful, an empty string, if not executed, null, else error
    fullscanTime INTEGER -- time when finishes last full scan
);""")

app = web.Flask(__name__)

@app.route("/test")
def test(): return "ok"

def sendAiServer(userId, js): return requests.post(f"{aiServer}/ingest?token=" + k1.aes_encrypt_json({"serverName": "yt", "userId": userId, "timeout": int(time.time()) + 20}), json=js)
def tokenGuard(args, request):
    token = args.get('token', default=None); redirect = lambda reason: web.redirect(f"{aiServer}/login?token=" + k1.aes_encrypt_json({"url": f"{ytServer}{request.full_path.strip('?')}", "tokenDuration": 86400, "timeout": int(time.time()) + 20}))
    if not token: redirect("Token not found")
    obj = k1.aes_decrypt_json(token)
    if time.time() > obj["timeout"]: redirect("Token timed out")
    if "userId" in obj:
        userId = obj["userId"]; user = db["users"].lookup(id=userId)
        if user is None:
            res = sendAiServer(userId, {"cmd": "newSchedule", "title": "Youtube summaries (yt app)"})
            if not res.ok: web.unauthorized(f"Tried to initialize user {userId} on {aiServer} but can't for some reason: {res.text}")
            db["users"].insert(id=userId, scheduleId=int(res.text))
    obj["token"] = token; return obj
def adminGuard(args, request):
    obj = tokenGuard(args, request)
    if obj.get("userId", 0) != 1: web.unauthorized()

def vidGuard(args, vidId, request):
    obj = tokenGuard(args, request)
    if db["access"].lookup(vidId=vidId, userId=obj["userId"]) is None: web.unauthorized("User not authorized to view/modify this video")
    return obj

@app.route("/", daisyEnv=True, guard=tokenGuard)
def index(guardRes):
    pre = init._jsDAuto(); user = db["users"][guardRes['userId']]
    sel = ["null", *db.query("select handle from channels") | cut(0)] | insId() | ~apply(lambda i, x: f"<option value='{x}' {'selected' if i == 0 else ''}>{x}</option>") | join("")
    return f"""<style>#main {{ flex-direction: column-reverse; }} @media (min-width: 600px) {{ #main {{ flex-direction: row; }} }}</style><title>Local youtube service</title>
<div id="main" style="display: flex; flex-direction: column">
    <div style="display: flex; flex-direction: row; align-items: center; margin-bottom: 24px">
        <h2>Videos</h2>
        <input id="{pre}_url" class="input input-bordered" placeholder="(video url)" style="margin-left: 24px; margin-right: 8px" autofocus onkeydown="if(event.key == 'Enter') {pre}_new();" />
        <button class="btn btn-outline" style="padding: 8px; margin-right: 4px; display: block" onclick="{pre}_new()">{k1.Icon.add()}</button>
        <button class="btn btn-outline" style="padding: 8px; margin-right: 4px; display: block" onclick="window.open('{aiServer}/schedule/{user.scheduleId}/search', '_blank');" title="Go to schedule search page">{k1.Icon.search()}</button>
    </div>
    <div style="display: flex; flex-direction: row; align-items: center; gap: 8px"><div>Channel</div>
        <select id="{pre}_sel" class="select input-bordered" style="width: fit-content">{sel}</select></div>
    <div id="{pre}_table" style="overflow-x: auto; width: 100%"></div>
    <div id="{pre}_res"></div></div>{fragment_channels(user, guardRes)}
<script>
    function vid_select(row, i, e) {{ dynamicLoad("#{pre}_res", `/mfragment/vid/${{row[0]}}?token={guardRes['token']}`); }}
    async function {pre}_new() {{ await wrapToastReq(fetchPost("/api/vid/new?token={guardRes['token']}", {{ url: {pre}_url.value.trim() }})); {pre}_url.value = ""; {pre}_url.focus(); }}
    {pre}_sel.oninput = () => {{ dynamicLoad("#{pre}_table", `/fragment/vids/${{{pre}_sel.value}}?token={guardRes['token']}`); }}; {pre}_sel.oninput();
</script>"""

@app.route("/fragment/vids/<handle>", guard=tokenGuard)
def fragment_vids(handle, guardRes):
    clause = "and c.handle is null" if handle == "null" else f"and c.handle = '{handle}'"; user = db["users"][guardRes['userId']]
    return db.query(f"select v.id, v.vidId, v.vidErr, v.transErr, v.duration, a.chatId, v.cleaned, a.archived, c.handle, v.vidTime, v.title from videos v join access a on v.id = a.vidId left join channels c on v.channelId = c.id where userId = ? {clause} order by a.chatId desc", user.id) | ~apply(lambda i,vi,ve,te,dur,cId,cls,ar,ha,vt,ti: [i,vi,
            'none' if ve is None else ('error' if ve else 'yes'),
            'none' if te is None else ('error' if te else 'yes'),dur,
            'none' if cId is None else ('error' if isinstance(cId, str) else cId),cls,ar,ha,vt | (tryout() | toIso() | op().replace(*"T ")),ti])\
        | deref() | (toJsFunc("term", ("archived", ["true", "false", "both"], "false")) | grep("${term}") | filt("x if archived == 'true' else ((not x) if archived == 'false' else true)", 7)\
            | (shape(0) | aS('f"#rows: {x}"')) & viz.Table(["id", "vidId", "hasVid", "hasTrans", "duration", "chatId", "cleaned", "archived", "handle", "vidTime", "title"], onclickFName=f"vid_select", selectable=True, height=400)) | op().interface() | toHtml()

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

def fragment_channels(user, guardRes): pre = init._jsDAuto(); ui1 = db.query(f"select id, provider, handle, fullscanErr from channels") | apply(lambda x: 'null' if x is None else ("yes" if x == "" else "error"), 3) | deref() | (toJsFunc("term") | grep("${term}") | viz.Table(["id", "provider", "handle", "fullscanErr"], height=400, onclickFName=f"{pre}_select", selectable=True)) | op().interface() | toHtml(); return f"""
<div style="display: flex; flex-direction: row; align-items: center; margin-bottom: 24px; margin-top: 12px">
    <h2>Channels</h2>
    <input id="{pre}_url" class="input input-bordered" placeholder="(video url)" style="margin-left: 24px; margin-right: 8px" autofocus onkeydown="if(event.key == 'Enter') {pre}_new();" />
    <button class="btn btn-outline" style="padding: 8px; margin-right: 4px; display: block" onclick="{pre}_new()">{k1.Icon.add()}</button></div>
<div style="overflow-x: auto; width: 100%">{ui1}</div><div id="{pre}_res"></div></div>
<script>function {pre}_select(row, i, e) {{ dynamicLoad("#{pre}_res", `/fragment/channel/${{row[0]}}?token={guardRes['token']}`); }}
async function {pre}_new() {{ await wrapToastReq(fetchPost(`/api/channel/new?token={guardRes['token']}`, {{ url: {pre}_url.value.trim() }})); {pre}_url.value = ""; {pre}_url.focus(); }}</script>"""

@app.route("/api/channel/new", methods=["POST"], guard=tokenGuard)
def api_channel_new(js, guardRes):
    url = js["url"]; userId = guardRes["userId"]; handle = None
    if "youtube.com/" in url:
        if url.startswith("https://www.youtube.com/@"): handle = "@" + url.split("https://www.youtube.com/@")[1].split("/")[0]
        else: web.toast_error(f"Invalid youtube channel link")
    if handle is None: web.toast_error("Provider not found")
    channel = db["channels"].lookup(handle=handle)
    if channel: web.toast_error("Channel added before!")
    db["channels"].insert(provider="yt", handle=handle, name=""); return "ok"
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
    warnings.warn(f"finished, og es: {len(es)}, urls: {len(urls)}"); return urls
@app.route("/fragment/channel/<int:channelId>", guard=tokenGuard)
def fragment_channel(channelId, guardRes): pre = init._jsDAuto(); channel = db["channels"][channelId]; return f"""<h3>Channel {channel.handle}</h3>
<button class="btn btn-outline" onclick="{pre}_fullScan()"    title="Scan for all videos on the channel, and add each video to the system">Full scan</button>
<button class="btn btn-outline" onclick="{pre}_partialScan()" title="Scan till there's a farmiliar video, then stop">Partial scan</button>
<script>function {pre}_fullScan() {{ wrapToastReq(fetch(`/api/channel/{channel.id}/fullScan?token={guardRes['token']}`)); }}
function {pre}_partialScan() {{ wrapToastReq(fetch(`/api/channel/{channel.id}/partialScan?token={guardRes['token']}`)); }}</script>"""

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
        b.pickExtFromGroup("yttri"); b.goto(f"https://www.youtube.com/{channel.handle}/videos"); time.sleep(1)
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

@k1.cron(delay=10)
def channel_fullscan_loop():
    for channel in db["channels"].select("where fullscanErr is null limit 1"):
        try: api_channel_fullScan(channel.id, {"userId": 1}); channel.fullscanErr = ""; channel.fullscanTime = int(time.time())
        except Exception as e: channel.fullscanErr = f"{type(e)} | {e}\n{traceback.format_exc()}"

@k1.cron(delay=60)
def archivedSyncLoop():
    chatIds = db.query("select chatId from access where (archived is null or archived = 0) and chatId is not null") | cut(0) | aS(list)
    for chatId, archivedTime in sendAiServer(1, {"cmd": "syncArchived", "chatIds": chatIds}).json():
        access = db["access"].lookup(chatId=chatId); access.archived = 1; access.archivedTime = archivedTime

@app.route("/raw/vid/<int:vidId>", guard=vidGuard)
def raw_vid(vidId):
    vid = db["videos"][vidId]
    with open(f"vids/{vid.vidId}", "rb") as f: return f.read()

@app.route("/api/vid/<vidId>/transcript", guard=vidGuard)
@app.route("/api/vid/<vidId>/transcript/<format>", guard=vidGuard)
def api_vid_transcript(vidId, format="text"):
    vid = db["videos"].lookup(vidId=vidId)
    if vid is None: web.notFound()
    if vid.transErr != "": web.notFound()
    if format == "vtt": return vid.trans
    return vid.trans.split("\n") | ~head(3) | batched(3) | item().all() | join("\n")

@app.route("/api/vids/recents", guard=tokenGuard)
def api_vids_recents(): return db.query("select vidId, title, duration from videos where transErr = '' order by id desc") | ~apply(lambda i,t,d: {"vidId": i, "title": t, "duration": d}) | aS(list) | aS(json.dumps)

@app.route("/fragment/vid/<int:vidId>", guard=vidGuard)
def fragment_vid(vidId, guardRes):
    vid = db["videos"][vidId]
    if vid.cleaned: return "(video was cleaned, no copies remained)"
    if vid.vidErr == "": return f"""<video controls width="640" height="360"><source src="/raw/vid/{vid.id}?token={guardRes['token']}" type="{vid.mime}">Your browser does not support the video tag.</video>"""
    return "(no video)"

@app.route("/mfragment/vid/<int:vidId>", guard=vidGuard)
def mfragment_vid(vidId, guardRes):
    pre = init._jsDAuto(); vid = db["videos"][vidId]; vidTag = ""; transTag = ""; chatTag = ""; access = db["access"].lookup(vidId=vid.id, userId=guardRes["userId"]); user = db["users"][access.userId]
    if vid.transErr == "": transTag = f"""<textarea class="textarea textarea-bordered" style="width: 100%; height: 360px">{vid.trans}</textarea>"""
    if isinstance(access.chatId, int): chatTag = f" - <a href='{aiServer}/schedules/{user.scheduleId}/{access.chatId}' target='_blank' style='color: blue'>Summary</a>"
    return f"""<style>#{pre}_main {{ flex-direction: row; }} @media (max-width: 800px) {{ #{pre}_main {{ flex-direction: column }} }}</style>
<h2><a href="{vid.url}" target="_blank">{vid.title}</a>{chatTag}</h2>
<div id="{pre}_main" style="display: flex; gap: 12px">
    <div style="flex: 1">{transTag}</div>
    <div id="vidHolder" style="flex: 1; display: grid; grid-template-columns: min-content auto; height: min-content; row-gap: 8px; column-gap: 8px; align-items: center">
        <button class="btn" onclick="vidHolder.style.display = 'block'; vidHolder.style.height = '360px'; dynamicLoad('#vidHolder', '/fragment/vid/{vid.id}?token={guardRes['token']}')">Load video</button><div></div>
        <button class="btn" onclick="wrapToastReq(fetch('/api/vid/{vid.id}/clear/vidErr?token={guardRes['token']}'))"  >Clear vidErr  </button><div id="{pre}_1"></div>
        <button class="btn" onclick="wrapToastReq(fetch('/api/vid/{vid.id}/clear/transErr?token={guardRes['token']}'))">Clear transErr</button><div id="{pre}_2"></div>
        <button class="btn" onclick="wrapToastReq(fetch('/api/vid/{vid.id}/clear/chatId?token={guardRes['token']}'))"  >Clear chatId  </button><div id="{pre}_3"></div>
        <button class="btn" onclick="wrapToastReq(fetch('/api/vid/{vid.id}/clear/title?token={guardRes['token']}'))"   >Clear title   </button><div id="{pre}_4"></div>
        <button class="btn" onclick="wrapToastReq(fetch('/api/vid/{vid.id}/clear/retain?token={guardRes['token']}'))"  >Retain        </button><div>Retain video, dont delete to save space</div>
    </div></div>
<script>{pre}_1.innerHTML = {json.dumps(vid.vidErr)}; {pre}_2.innerHTML = {json.dumps(vid.transErr)};
{pre}_3.innerHTML = {json.dumps(access.chatId)}; {pre}_4.innerHTML = {json.dumps(vid.title)};</script>"""

@app.route("/api/vid/<int:vidId>/clear/<resource>", guard=vidGuard)
def api_vid_clear(vidId, resource, guardRes):
    vid = db["videos"][vidId]
    if resource == "vidErr":   vid.vidErr = None
    if resource == "transErr": vid.transErr = None
    if resource == "chatId":
        access = db["access"].lookup(vidId=vid.id, userId=guardRes['userId'])
        if isinstance(access.chatId, int):
            res = sendAiServer(access.userId, {"cmd": "deleteChat", "chatId": access.chatId}) # deletes old chat from ai server, to prevent clogging things up
            if not res.ok or res.text.strip() != "ok": web.toast_error(f"Can't delete chat on {aiServer}")
        access.chatId = None
    if resource == "title":    vid.title = None
    if resource == "retain":   vid.retain = True
    return "ok"

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
    for accessId, vidId in db.query("select a.id, v.id from videos v join access a on v.id = a.vidId where v.transErr = '' and a.chatId is null order by a.id limit 1"):
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

ytUrl = f"http://localhost:5008"
@toolCatchErr
def ytRecents() -> dict:
    """Get recently downloaded youtube videos metadata"""
    yield None; return requests.get(f"{ytUrl}/api/vids/recents").text | aS(json.loads)
@toolCatchErr
def ytTranscript(vidId:str, env) -> str:
    """Get transcript of specific youtube video"""
    yield {"type": "status", "content": "Fetching transcript"}
    return api_vid_transcript(vidId, "vtt")

toolsD = {"ytTranscript": ytTranscript}

@app.route("/ingest", methods=["POST"], guard=tokenGuard)
def ingest(js):
    if js["cmd"] == "toolCall":
        func = js["func"]; env = js["env"]; args = js["args"]
        if func in toolsD:
            it = ytTranscript(**{**args, "env": env})
            try:
                while True: x = next(it); yield (json.dumps({"type": "yield", "value": x}) | toBase64()) + "\n"
            except StopIteration as e: yield (json.dumps({"type": "return", "value": e.value}) | toBase64()) + "\n"
            return ""
    web.notFound("Don't understand this ingest message")

@app.route("/serverDef")
def serverDef(): # server definition so that it can be used by main ai server
    tools = [ytTranscript] | apply(function_to_ollama_tool) | apply(lambda x: {"server": "yt", "schema": x}) | aS(list)
    res = {"url": ytServer, "name": "yt", "descr": "Manages youtube downloads", "tools": tools, "userMode": "mirror"}; return json.dumps(res)

@app.route("/api/restart", guard=adminGuard)
def restart(): None | cmd("touch main.py") | ignore(); return "ok", 200, {"Access-Control-Allow-Origin": "*"}

@app.route("/metrics")
def metrics():
    s = ""
    res = db.query("select count(id) from videos where vidErr = ''")   [0][0]; s += f'yt_vid_count{{status="done"}} {res}\n'
    res = db.query("select count(id) from videos where vidErr is null")[0][0]; s += f'yt_vid_count{{status="not started"}} {res}\n'
    res = db.query("select count(id) from videos where vidErr != ''")  [0][0]; s += f'yt_vid_count{{status="error"}} {res}\n'
    res = db.query("select count(id) from videos where transErr = ''")   [0][0]; s += f'yt_trans_count{{status="done"}} {res}\n'
    res = db.query("select count(id) from videos where transErr is null")[0][0]; s += f'yt_trans_count{{status="not started"}} {res}\n'
    res = db.query("select count(id) from videos where transErr != ''")  [0][0]; s += f'yt_trans_count{{status="error"}} {res}\n'
    for count, status in db.query("select count(id), typeof(chatId) as ct from access group by ct") | lookup({"integer": "done", "null": "not started", "text": "error"}, 1):
        s += f'yt_chats_count{{status="{status}"}} {count}\n'
    for userId, archivedTime in db.query("select a.userId, sum(v.duration) from videos v join access a on v.id = a.vidId where a.archived = 1 group by a.userId"):
        s += f'yt_archivedTime_total{{userId="{userId}"}} {archivedTime}\n'
    for userId, nChats in db.query("select a.userId, count(v.duration) from videos v join access a on v.id = a.vidId where a.archived = 1 group by a.userId"):
        s += f'yt_archivedChats_count{{userId="{userId}"}} {nChats}\n'
    return s

sql.lite_flask(app, guard=adminGuard); k1.logErr.flask(app, guard=adminGuard); k1.cron.flask(app, guard=adminGuard)

app.run(host="0.0.0.0", port=5008) # same as normal flask code








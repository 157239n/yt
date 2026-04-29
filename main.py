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
    cleaned     BOOL     -- whether this video has been removed to save space?
);""")
db.query("CREATE INDEX IF NOT EXISTS videos_vidId ON videos (vidId);")
db.query("""CREATE TABLE IF NOT EXISTS users ( -- this is just to keep track of what user has been initialized
    id          INTEGER primary key, -- no autoincrement because ground truth is on ai server's db
    scheduleId  INTEGER              -- scheduleId just for this youtube app
);""")
db.query("""CREATE TABLE IF NOT EXISTS access ( -- track what user has access to what videos
    id          INTEGER primary key, -- no autoincrement because ground truth is on ai server's db
    vidId       INTEGER,
    userId      INTEGER,
    chatId      INTEGER  -- chatId for this particular video summary. Can be a string for error message
);""")
db.query("CREATE INDEX IF NOT EXISTS access_vidId ON access (vidId);")
db.query("CREATE INDEX IF NOT EXISTS access_userId ON access (userId);")

app = web.Flask(__name__)

@app.route("/test")
def test(): return "ok"

def sendAiServer(js): return requests.post(f"{aiServer}/ingest?token=" + k1.aes_encrypt_json({"app": "yt", "timeout": int(time.time()) + 20}), json=js)
def tokenGuard(args, request):
    token = args.get('token', default=None); redirect = lambda reason: web.redirect(f"{aiServer}/login?token=" + k1.aes_encrypt_json({"url": f"{ytServer}{request.full_path.strip('?')}", "tokenDuration": 86400}))
    if not token: redirect("Token not found")
    obj = k1.aes_decrypt_json(token)
    if time.time() > obj["timeout"]: redirect("Token timed out")
    if "userId" in obj:
        userId = obj["userId"]; user = db["users"].lookup(id=userId)
        if user is None:
            res = sendAiServer({"cmd": "newSchedule", "userId": userId, "title": "Youtube summaries"})
            if not res.ok: web.unauthorized(f"Tried to initialize user {userId} on {aiServer} but can't for some reason: {res.text}")
            db["users"].insert(id=userId, scheduleId=int(res.text))
    obj["token"] = token; return obj

def vidGuard(args, vidId, request):
    obj = tokenGuard(args, request)
    if db["access"].lookup(vidId=vidId, userId=obj["userId"]) is None: web.unauthorized("User not authorized to view/modify this video")
    return obj

@app.route("/", daisyEnv=True, guard=tokenGuard)
def index(guardRes):
    pre = init._jsDAuto(); user = db["users"][guardRes['userId']]
    ui1 = db.query(f"select v.id, v.vidId, v.vidErr, v.transErr, v.duration, a.chatId, v.cleaned, v.title from videos v join access a on v.id = a.vidId where userId = ? order by v.id desc", user.id) | ~apply(lambda i,vi,ve,te,dur,cId,cls,ti: [i,vi,
            'none' if ve is None else ('error' if ve else 'yes'),
            'none' if te is None else ('error' if te else 'yes'),dur,
            'none' if cId is None else ('error' if isinstance(cId, str) else cId),cls,ti])\
        | deref() | (toJsFunc("term") | grep("${term}") | viz.Table(["id", "vidId", "hasVid", "hasTrans", "duration", "chatId", "cleaned", "title"], onclickFName=f"{pre}_select", selectable=True, height=400)) | op().interface() | toHtml()
    return f"""<style>#main {{ flex-direction: column-reverse; }} @media (min-width: 600px) {{ #main {{ flex-direction: row; }} }}</style><title>Local youtube service</title>
<div id="main" style="display: flex; flex-direction: column">
    <div style="display: flex; flex-direction: row; align-items: center; margin-bottom: 24px">
        <h2>Videos</h2>
        <input id="{pre}_url" class="input input-bordered" placeholder="(video url)" style="margin-left: 24px; margin-right: 8px" autofocus />
        <button class="btn btn-outline" style="padding: 8px; margin-right: 4px; display: block" onclick="{pre}_new()" onkeydown="if(event.key == 'Enter') {pre}_new();">{k1.Icon.add()}</button>
        <button class="btn btn-outline" style="padding: 8px; margin-right: 4px; display: block" onclick="window.open('{aiServer}/schedule/{user.scheduleId}/search', '_blank');" title="Go to schedule search page">{k1.Icon.search()}</button>
    </div>
    <div style="overflow-x: auto; width: 100%">{ui1}</div>
    <div id="{pre}_res"></div></div>
<script>
    function {pre}_select(row, i, e) {{ dynamicLoad("#{pre}_res", `/mfragment/vid/${{row[0]}}?token={guardRes['token']}`); }}
    async function {pre}_new() {{ await wrapToastReq(fetchPost("/api/vid/new?token={guardRes['token']}", {{ url: {pre}_url.value.trim() }})); {pre}_url.value = ""; {pre}_url.focus(); }}
</script>"""

@app.route("/api/vid/new", methods=["POST"], guard=tokenGuard)
def api_vid_new(js, guardRes):
    url = js["url"]; provider = None; userId = guardRes["userId"]; vidId = None
    if url.startswith("https://www.youtube.com/watch"):
        provider = "yt"; vidId = url.split("/watch")[-1].strip("?").split("&")[0]; vidId = parse_qs(urlparse(url).query).get("v", [None])[0]
    elif url.startswith("https://www.dailymotion.com/video"):
        provider = "dailymotion"; vidId = url.split("/video/")[1].split("/")[0].split("?")[0].split("&")[0].strip()
    if vidId is None: web.toast_error("Can't extract vidId")
    if provider is None: web.toast_error("Don't know what service (youtube, dailymotion, etc) this url belongs to")
    vid = db["videos"].lookup(vidId=vidId, provider=provider)
    if vid:
        hasAccess = db["access"].lookup(vidId=vid.id, userId=userId)
        if hasAccess: web.toast_error("Video added before!")
        else: db["access"].insert(vidId=vid.id, userId=userId, chatId=None); return "ok"
    vid = db["videos"].insert(url=url, vidId=vidId, title=None, vidErr=None, trans="", transErr=None, createdTime=int(time.time()), provider=provider, retain=0, cleaned=0)
    db["access"].insert(vidId=vid.id, userId=userId, chatId=None); return "ok"

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
            res = sendAiServer({"cmd": "deleteChat", "chatId": access.chatId}) # deletes old chat from ai server, to prevent clogging things up
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

@k1.cron(delay=60)
def vidLoop(): # auto detects videos that need to be taken care of
    for vid in db["videos"].select("where vidErr is null limit 1"):
        print(f"vid: {vid.id}, provider: {vid.provider}")
        if vid.provider in providers:
            res = None | cmd(f'{yt_dlp} --cookies cookies.txt -o "tmpVids/new.%(ext)s" {providers[vid.provider]}{vid.vidId}', mode=0) | deref()
            fns = "tmpVids" | ls() | grep("new") | deref()
            if len(fns) == 0: vid.vidErr = f"Tried to download, no new.mp4 or new.webm or others found in tmpVids: {res}"; continue
            None | cmd(f"mv {fns[0]} vids/{vid.vidId}") | ignore(); vid.vidErr = ""
        else: vid.vidErr = f"Unknown provider '{vid.provider}'"

@k1.cron(delay=10)
def magicLoop():
    for vid in db["videos"].select("where vidErr='' and mime is null"):
        print(f"magic: {vid.id}")
        with open(f"vids/{vid.vidId}", "rb") as f: vid.mime = magic.Magic(mime=True).from_buffer(f.read())

@k1.cron(delay=10)
def matroskaLoop(): # detects videos that are of matroska format, and reformat it to webm so browsers can actually play it
    return # mostly discarded anyway, so let's not waste cpu cycles on this
    for vid in db["videos"].select("where mime = 'video/x-matroska'"):
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
    for vid in db["videos"].select("where vidErr = '' and transErr is null"):
        print(f"transcribe: {vid.id}")
        try: vid.trans = getVtt(model.transcribe(f"vids/{vid.vidId}", beam_size=1, batch_size=8)[0]); vid.transErr = ""
        except Exception as e: vid.transErr = f"Tried to transcribe, encountered error: {type(e)} | {e} | {traceback.format_exc()}"

@k1.cron(delay=10)
def summarizeLoop():
    # if len(db.query("select id from videos where vidErr = '' and transErr is null")) > 0: print("skipping summarize loop"); return # when there's stuff to transcribe, it consumes the gpu, which means text generation is going to be slow, so pause summarization if there's transcription going on
    for accessId, vidId in db.query("select a.id, v.id from videos v join access a on v.id = a.vidId where v.transErr = '' and a.chatId is null"):
        access = db["access"][accessId]
        try:
            vid = db["videos"][vidId]; user = db["users"][access.userId]; print(f"summarize: {vid.id}")
            res = sendAiServer({"cmd": "scheduleNewChat", "scheduleId": user.scheduleId, "prompt": f"Please fetch the transcript of youtube video with id '{vid.vidId}' (title '{vid.title}', duration {vid.duration}) and summarize 2 times, once for a 1-2 paragraph  overview, and once in detail. Transcript might have small spelling errors (but not core facts), correct it if necessary"})
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
    return api_vid_transcript(vidId)

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
    res = {"url": ytServer, "name": "yt", "descr": "Manages youtube downloads", "tools": tools}; return json.dumps(res)

sql.lite_flask(app); k1.logErr.flask(app); k1.cron.flask(app)

app.run(host="0.0.0.0", port=5008) # same as normal flask code








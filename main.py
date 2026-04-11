from k1lib.imports import *
import magic
from urllib.parse import urlparse, parse_qs

yt_dlp = "/home/kelvin/envs/torch/bin/yt-dlp"
whisper = "/home/kelvin/envs/torch/bin/whisper"

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
    chatStr     TEXT     -- scheduleId/chatId on ai server summarizing the video
);""")
db.query("CREATE INDEX IF NOT EXISTS videos_vidId ON videos (vidId);")

app = web.Flask(__name__)

@app.route("/", daisyEnv=True)
def index():
    pre = init._jsDAuto()
    ui1 = db.query("select id, vidId, vidErr, transErr, duration, chatStr, title from videos order by id desc") | ~apply(lambda i,vi,ve,te,dur,cs,ti: [i,vi,
            'none' if ve is None else ('error' if ve else 'yes'),
            'none' if te is None else ('error' if te else 'yes'),dur,
            'none' if cs is None else ('error' if cs.startswith("error") else cs),ti])\
        | deref() | (toJsFunc("term") | grep("${term}") | viz.Table(["id", "vidId", "hasVid", "hasTrans", "duration", "chatStr", "title"], onclickFName=f"{pre}_select", selectable=True, height=400)) | op().interface() | toHtml()
    return f"""<style>#main {{ flex-direction: column-reverse; }} @media (min-width: 600px) {{ #main {{ flex-direction: row; }} }}</style><title>Local youtube service</title>
<div id="main" style="display: flex; flex-direction: column">
    <div style="display: flex; flex-direction: row; align-items: center; margin-bottom: 24px">
        <h2>Videos</h2>
        <input id="{pre}_url" class="input input-bordered" placeholder="(video url)" style="margin-left: 24px" />
        <button id="{pre}_newBtn" class="btn">{k1.Icon.add()}</button>
    </div>
    <div style="overflow-x: auto; width: 100%">{ui1}</div>
    <div id="{pre}_res"></div></div>
<script>
    function {pre}_select(row, i, e) {{ dynamicLoad("#{pre}_res", `/mfragment/vid/${{row[0]}}`); }}
    {pre}_newBtn.onclick = async () => {{ await wrapToastReq(fetchPost("/api/vid/new", {{ url: {pre}_url.value.trim() }})); {pre}_url.value = ""; }}
</script>"""

@app.route("/api/vid/new", methods=["POST"])
def api_vid_new(js):
    url = js["url"]; vidId = url.split("/watch")[-1].strip("?").split("&")[0]; vidId = parse_qs(urlparse(url).query).get("v", [None])[0]
    if vidId is None: web.toast_error("Can't extract vidId")
    if db["videos"].lookup(vidId=vidId): web.toast_error("Video added before!")
    db["videos"].insert(url=url, vidId=vidId, title=None, vidErr=None, trans="", transErr=None, createdTime=int(time.time())); return "ok"

@app.route("/raw/vid/<int:vidId>")
def raw_vid(vidId):
    vid = db["videos"][vidId]
    with open(f"vids/{vid.vidId}", "rb") as f: return f.read()

@app.route("/api/vid/<vidId>/transcript")
@app.route("/api/vid/<vidId>/transcript/<format>")
def api_vid_transcript(vidId, format="text"):
    vid = db["videos"].lookup(vidId=vidId)
    if vid is None: web.notFound()
    if vid.transErr != "": web.notFound()
    if format == "vtt": return vid.trans
    return vid.trans.split("\n") | ~head(3) | batched(3) | item().all() | join("\n")

@app.route("/api/vids/recents")
def api_vids_recents(): return db.query("select vidId, title, duration from videos where transErr = '' order by id desc") | ~apply(lambda i,t,d: {"vidId": i, "title": t, "duration": d}) | aS(list) | aS(json.dumps)

@app.route("/fragment/vid/<int:vidId>")
def fragment_vid(vidId):
    vid = db["videos"][vidId]
    if vid.vidErr == "": return f"""<video controls width="640" height="360"><source src="/raw/vid/{vid.id}" type="{vid.mime}">Your browser does not support the video tag.</video>"""
    return "(no video)"

@app.route("/mfragment/vid/<int:vidId>")
def mfragment_vid(vidId):
    pre = init._jsDAuto(); vid = db["videos"][vidId]; vidTag = ""; transTag = ""; chatTag = ""
    if vid.transErr == "": transTag = f"""<textarea class="textarea textarea-bordered" style="width: 100%; height: 360px">{vid.trans}</textarea>"""
    if vid.chatStr and not vid.chatStr.startswith("error"): chatTag = f" - <a href='https://ai.aigu.vn/schedules/{vid.chatStr}' target='_blank' style='color: blue'>Summary</a>"
    return f"""<style>#{pre}_main {{ flex-direction: row; }} @media (max-width: 800px) {{ #{pre}_main {{ flex-direction: column }} }}</style>
<h2><a href="{vid.url}" target="_blank">{vid.title}</a>{chatTag}</h2>
<div id="{pre}_main" style="display: flex; gap: 12px">
    <div style="flex: 1">{transTag}</div>
    <div id="vidHolder" style="flex: 1; display: grid; grid-template-columns: min-content auto; height: min-content; row-gap: 8px; column-gap: 8px; align-items: center">
        <button class="btn" onclick="vidHolder.style.display = 'block'; vidHolder.style.height = '360px'; dynamicLoad('#vidHolder', '/fragment/vid/{vid.id}')">Load video</button><div></div>
        <button class="btn" onclick="wrapToastReq(fetch('/api/vid/{vid.id}/clear/vidErr'))"  >Clear vidErr  </button><div id="{pre}_1"></div>
        <button class="btn" onclick="wrapToastReq(fetch('/api/vid/{vid.id}/clear/transErr'))">Clear transErr</button><div id="{pre}_2"></div>
        <button class="btn" onclick="wrapToastReq(fetch('/api/vid/{vid.id}/clear/chatStr'))" >Clear chatStr </button><div id="{pre}_3"></div>
        <button class="btn" onclick="wrapToastReq(fetch('/api/vid/{vid.id}/clear/title'))"   >Clear title   </button><div id="{pre}_4"></div>
    </div></div>
<script>{pre}_1.innerHTML = {json.dumps(vid.vidErr)}; {pre}_2.innerHTML = {json.dumps(vid.transErr)};
{pre}_3.innerHTML = {json.dumps(vid.chatStr)}; {pre}_4.innerHTML = {json.dumps(vid.title)};</script>"""

@app.route("/api/vid/<int:vidId>/clear/<resource>")
def api_vid_clear(vidId, resource):
    vid = db["videos"][vidId]
    if resource == "vidErr":   vid.vidErr = None
    if resource == "transErr": vid.transErr = None
    if resource == "chatStr":  vid.chatStr = None
    if resource == "title":    vid.title = None
    return "ok"

@k1.cron(delay=10)
def titleLoop():
    for vid in db["videos"].select("where title is null"):
        vid.title = None | cmd(f'{yt_dlp} --cookies cookies.txt --print "%(title)s" https://www.youtube.com/watch?v={vid.vidId}') | deref() | join("\n")

@k1.cron(delay=10)
def vidLoop(): # auto detects videos that need to be taken care of
    for vid in db["videos"].select("where vidErr is null limit 1"):
        print(f"vid: {vid.id}")
        res = None | cmd(f'{yt_dlp} --cookies cookies.txt -o "tmpVids/new.%(ext)s" https://www.youtube.com/watch?v={vid.vidId}', mode=0) | deref()
        fns = "tmpVids" | ls() | grep("new") | deref()
        if len(fns) == 0: vid.vidErr = "Tried to download, no new.mp4 or new.webm or others found in tmpVids"; print(f"vidLoop error: {res}"); continue
        None | cmd(f"mv {fns[0]} vids/{vid.vidId}") | ignore(); vid.vidErr = ""

@k1.cron(delay=10)
def magicLoop():
    for vid in db["videos"].select("where vidErr='' and mime is null"):
        print(f"magic: {vid.id}")
        with open(f"vids/{vid.vidId}", "rb") as f: vid.mime = magic.Magic(mime=True).from_buffer(f.read())

@k1.cron(delay=10)
def matroskaLoop(): # detects videos that are of matroska format, and reformat it to webm so browsers can actually play it
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

@k1.cron(delay=10)
def transLoop():
    for vid in db["videos"].select("where vidErr = '' and transErr is null"):
        print(f"transcribe: {vid.id}")
        res = None | cmd(f'{whisper} vids/{vid.vidId} -f vtt --output_dir tmpTrans --model small', mode=0) | deref()
        fns = "tmpTrans" | ls() | grep(vid.vidId) | deref()
        if len(fns) == 0: vid.transErr = "Tried to transcribe, no vidId.vtt found in tmpTrans"; warnings.warn(f"trans error: {res}"); return
        with open(fns[0]) as f: vid.trans = f.read(); vid.transErr = ""
        None | cmd(f'rm tmpTrans/*') | ignore()

@k1.cron(delay=10)
def summarizeLoop():
    if len(db.query("select id from videos where vidErr = '' and transErr is null")) > 0: return # when there's stuff to transcribe, it consumes the gpu, which means text generation is going to be slow, so pause summarization if there's transcription going on
    for vid in db["videos"].select("where transErr = '' and chatStr is null"):
        print(f"summarize: {vid.id}")
        res = requests.post("https://ai.aigu.vn/api/schedule/18/manualNewChat", json={"prompt": f"Please fetch the transcript of youtube video with id '{vid.vidId}' (title '{vid.title}', duration {vid.duration}) and summarize it"})
        vid.chatStr = f"18/{res.text}" if res.ok else f"error: {res.text}"

sql.lite_flask(app); k1.logErr.flask(app); k1.cron.flask(app)

app.run(host="0.0.0.0", port=5008) # same as normal flask code








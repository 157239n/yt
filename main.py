from common import *

@app.route("/", daisyEnv=True, guard=tokenGuard)
def index(guardRes):
    pre = init._jsDAuto(); user = db["users"][guardRes['userId']]
    sel = ["null", *db.query("select handle from channels") | cut(0)] | insId() | ~apply(lambda i, x: f"<option value='{x}' {'selected' if i == 0 else ''}>{x}</option>") | join("")
    recents = db.query("select v.id, v.vidId, v.duration, a.chatId, v.vidTime, c.handle, v.title from videos v join access a on v.id = a.vidId left join channels c on v.channelId = c.id where a.chatId is not null and a.archived = 0 order by a.chatId desc limit 300")\
        | (toJsFunc("term") | grep("${term}") | viz.Table(["id", "vidId", "duration", "chatId", "vidTime", "handle", "title"], onclickFName="vid_select", selectable=True, height=400)) | op().interface() | toHtml()
    return f"""<style>#main {{ flex-direction: column-reverse; }} @media (min-width: 600px) {{ #main {{ flex-direction: row; }} }}</style><title>Local youtube service</title>
<div id="main" style="display: flex; flex-direction: column">
    <div style="display: flex; flex-direction: row">
        <div style="flex: 1; overflow-x: auto">
            <div style="display: flex; flex-direction: row; align-items: center; margin-bottom: 24px">
                <h2>Videos</h2>
                <input id="{pre}_url" class="input input-bordered" placeholder="(video url)" style="margin-left: 24px; margin-right: 8px" autofocus onkeydown="if(event.key == 'Enter') {pre}_new();" />
                <button class="btn btn-outline" style="padding: 8px; margin-right: 4px; display: block" onclick="{pre}_new()">{k1.Icon.add()}</button>
                <button class="btn btn-outline" style="padding: 8px; margin-right: 4px; display: block" onclick="window.open('{aiServer}/schedule/{user.scheduleId}/search', '_blank');" title="Go to schedule search page">{k1.Icon.search()}</button>
            </div>
            <div style="display: flex; flex-direction: row; align-items: center; gap: 8px"><div>Channel</div>
                <select id="{pre}_sel" class="select input-bordered" style="width: fit-content">{sel}</select></div>
            <div id="{pre}_table" style="overflow-x: auto; width: 100%"></div>
        </div>
        <div style="flex: 1; overflow-x: auto; display: flex; flex-direction: column">
            <div style="flex: 1"></div><h2>Recents</h2>{recents}</div>
    </div>
    <div id="{pre}_res"></div></div>
    {fragment_channels(user, guardRes)}
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

def fragment_channels(user, guardRes):
    pre = init._jsDAuto()
    d1, d2, d3 = db.query("select c.id, sum(a.archived), sum(coalesce(a.chatId, 0) > 0), count(a.id) from access a join videos v on a.vidId = v.id join channels c on v.channelId = c.id group by c.id") | ~apply(lambda a,b,c,d: [[a,b], [a,c], [a,d]]) | T() | toDict().all()
    ui1 = db.query(f"select id, provider, handle, fullscanErr, id, id, id from channels") | lookup(d1, 4, fill="(no data)") | lookup(d2, 5, fill="(no data)") | lookup(d3, 6, fill="(no data)") | apply(lambda arr: [*arr, round(arr[4]/arr[5]*100) if not isinstance(arr[5], str) and arr[5] > 0 else -1]) | apply(lambda x: 'null' if x is None else ("yes" if x == "" else "error"), 3) | deref()\
        | (toJsFunc("term") | grep("${term}") | viz.Table(["id", "provider", "handle", "fullscanErr", "archived", "ready", "total", "%read"], height=400, onclickFName=f"{pre}_select", selectable=True, sortF=True)) | op().interface() | toHtml(); return f"""
<div style="display: flex; flex-direction: row; align-items: center; margin-bottom: 24px; margin-top: 12px">
    <h2>Channels</h2>
    <input id="{pre}_url" class="input input-bordered" placeholder="(channel url)" style="margin-left: 24px; margin-right: 8px" autofocus onkeydown="if(event.key == 'Enter') {pre}_new();" />
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

@app.route("/fragment/channel/<int:channelId>", guard=tokenGuard)
def fragment_channel(channelId, guardRes): pre = init._jsDAuto(); channel = db["channels"][channelId]; return f"""<h3>Channel {channel.handle}</h3>
<div style="display: flex; flex-direction: row; gap: 8px">
    <button class="btn btn-outline" onclick="wrapToastReq(fetch(`/api/channel/{channel.id}/fullScan?token={guardRes['token']}`))"    title="Scan for all videos on the channel, and add each video to the system">Full scan</button>
    <button class="btn btn-outline" onclick="wrapToastReq(fetch(`/api/channel/{channel.id}/partialScan?token={guardRes['token']}`))" title="Scan till there's a farmiliar video, then stop">Partial scan</button>
    <button class="btn btn-outline" onclick="wrapToastReq(fetch(`/api/channel/{channel.id}/clearError?token={guardRes['token']}`))">Clear error</button></div>
<textarea id="{pre}_err" style="height: 400px; width: 100%; margin-top: 12px"></textarea><script>{pre}_err.value = {json.dumps(channel.fullscanErr)}</script>"""

@app.route("/api/channel/<int:channelId>/clearError", guard=tokenGuard)
def api_channel_clearError(channelId): db["channels"][channelId].fullscanErr = None; return "ok"

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








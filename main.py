# import sys; sys.exit(0)

from common import *
from runner import *

def preamble(darkmode): return f"""
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/github-markdown-css/5.5.0/github-markdown-{'dark' if darkmode else 'light'}.min.css">
<script src="https://cdn.jsdelivr.net/npm/marked/lib/marked.umd.js"></script>
<script>window.MathJax = {{ tex: {{ inlineMath: [['$', '$']], displayMath: [['$$', '$$']], processEscapes: true }} }};</script>
<script id="MathJax-script" src="https://cdn.jsdelivr.net/npm/mathjax@3/es5/tex-chtml.js"></script>
<style>.chunk > * {{ margin: 0px !important; }} .btn-multiline {{ height: auto !important; min-height: 2.5rem; white-space: normal !important; line-height: 1.3; padding-top: 0.5rem; padding-bottom: 0.5rem; }}
.markdown-body ol,.markdown-body ul{{margin:.5em 0 .5em 1.5em;padding-left:1.2em}}.markdown-body ul{{list-style:disc}}.markdown-body ul ul{{list-style:circle}}.markdown-body ul ul ul{{list-style:square}}.markdown-body ol{{list-style:decimal}}.markdown-body ol ol{{list-style:lower-alpha}}.markdown-body ol ol ol{{list-style:lower-roman}}.markdown-body li{{display:list-item;margin:.25em 0}}</style>
<style>textarea {{ resize: none; }} body, html {{ {'background: #0d1117; color: #e6edf3' if darkmode else ''} }}
.btn, input, .select, .textarea, .modal-box {{ {'background: #181C14 !important; color: #e6edf3 !important; border: #3C3D37 1px solid !important' if darkmode else ''} }}
*::-webkit-scrollbar {{ width: 6px; height: 6px; }} *::-webkit-scrollbar-track {{ background: transparent; }} *::-webkit-scrollbar-thumb {{ background: {'#444' if darkmode else '#888'}; border-radius: 3px; }}
table tr:first-child {{ background: {'#0d1117' if darkmode else '#e6edf3'} !important; }}
._k1_viz_Table_row_active_ {{ {'background: #181C14 !important;' if darkmode else ''} }}
.flex_row {{ display: flex; flex-direction: row; }} @media (max-width: 700px) {{ .flex_row {{ flex-direction: column; }} }}
</style>"""

@app.route("/", daisyEnv=True, guard=tokenGuard)
def index(guardRes):
    pre = init._jsDAuto(); user = db["users"][guardRes['userId']]; iconF = "#e6edf3" if guardRes["darkmode"] else "#333"
    sel = ["null", *db.query("select handle from channels") | cut(0), *db.query("select handle from playlists") | cut(0)] | insId() | ~apply(lambda i, x: f"<option value='{x}' {'selected' if i == 0 else ''}>{x}</option>") | join("")
    errors = db.query("""select v.id, v.vidId, a.chatId, v.duration, v.vidTime, c.handle, v.title from videos v
                              join access a on v.id = a.vidId left join channels c on v.channelId = c.id
                              where v.deleted = 0 and ((v.vidErr is not null and v.vidErr != '') or (v.transErr is not null and v.transErr != '') or (v.soundErr is not null and v.soundErr != '' and v.soundErr != '0') or typeof(v.duration) = 'text') and a.userId = ?""", user.id)
    recents = [*errors, *db.query("""select v.id, v.vidId, a.chatId, v.duration, v.vidTime, c.handle, v.title from videos v
                                        join access a on v.id = a.vidId left join channels c on v.channelId = c.id
                                        where v.deleted = 0 and a.chatId is not null and a.archived = 0 and a.userId = ? order by a.chatId desc limit 100""", user.id)]\
        | apply(tryout() | toIso() | op().replace(*"T "), 4) | (toJsFunc("term") | grep("${term}") | viz.Table(["id", "vidId", "chatId", "duration", "vidTime", "handle", "title"], onclickFName="vid_select", ondeleteFName="vid_delete", selectable=True, sortF=True, height=400)) | op().interface() | toHtml()
    return f"""<style>#main {{ flex-direction: column-reverse; }} @media (min-width: 600px) {{ #main {{ flex-direction: row; }} }}</style><title>Local youtube service</title>
{preamble(guardRes["darkmode"])}
<div id="main" style="display: flex; flex-direction: column">
    {fragment_channels(guardRes)}
    {fragment_playlists(guardRes)}
    <div class="flex_row" style="gap: 12px">
        <div style="flex: 1; overflow-x: auto">
            <div style="display: flex; flex-direction: row; align-items: center; flex-wrap: wrap; margin-bottom: 24px; padding-top: 12px">
                <h2>Videos</h2>
                <input id="{pre}_url" class="input input-bordered" placeholder="(video url)" style="margin-left: 24px; margin-right: 8px" autofocus onkeydown="if(event.key == 'Enter') {pre}_new();" />
                <button class="btn btn-outline" style="padding: 8px; margin-right: 4px; display: block" onclick="{pre}_new()">{k1.Icon.add(fill=iconF)}</button>
                <button class="btn btn-outline" style="padding: 8px; margin-right: 12px; display: block" onclick="window.open('{aiServer}/schedule/{user.scheduleId}/search', '_blank');" title="Go to schedule search page">{k1.Icon.search(fill=iconF)}</button>
                <div style="display: flex; flex-direction: row; align-items: center; gap: 8px"><div>Channel</div>
                    <select id="channel_sel" class="select input-bordered" style="width: 100px">{sel}</select></div>
            </div>
            <div id="{pre}_table" style="overflow-x: auto; width: 100%; padding-top: 4px"></div>
        </div>
        <div style="flex: 1; overflow-x: auto; display: flex; flex-direction: column">
            <div style="flex: 1"></div><h2>Recents</h2>{recents}</div>
    </div>
    <div id="{pre}_res"></div></div>
<script>
    function vid_select(row, i, e) {{
        if ((e.metaKey || e.ctrlKey || document.body.clientWidth < 700) && typeof(row[2]) == 'number') window.open(`{aiServer}/schedules/{user.scheduleId}/${{row[2]}}`);
        else dynamicLoad("#{pre}_res", `/mfragment/vid/${{row[0]}}?token={guardRes['token']}`); }}
    async function vid_delete(row, i, e) {{ await wrapToastReq(fetch(`/api/vid/${{row[0]}}/delete?token={guardRes['token']}`)); }}
    async function {pre}_new() {{ await wrapToastReq(fetchPost("/api/vid/new?token={guardRes['token']}", {{ url: {pre}_url.value.trim() }})); {pre}_url.value = ""; {pre}_url.focus(); }}
    channel_sel.oninput = () => {{ dynamicLoad("#{pre}_table", `/fragment/vids/${{channel_sel.value}}?token={guardRes['token']}`); }}; channel_sel.oninput();
</script>"""

@app.route("/fragment/vids/<handle>", guard=tokenGuard)
def fragment_vids(handle, guardRes):
    user = db["users"][guardRes['userId']]
    if handle == "null" or handle.startswith("@"): # real channel, or null (videos with no channels)
        clause = "and c.handle is null" if handle == "null" else f"and c.handle = '{handle}'"
        final = f"select v.id, v.vidId, a.chatId, v.vidErr, v.transErr, v.soundErr, v.duration, v.cleaned, a.archived, c.handle, v.vidTime, v.title from videos v join access a on v.id = a.vidId left join channels c on v.channelId = c.id where userId = {user.id} and v.deleted = 0 {clause} order by a.chatId desc"
    else: # playlists
        final = f"select v.id, v.vidId, a.chatId, v.vidErr, v.transErr, v.soundErr, v.duration, v.cleaned, a.archived, null, v.vidTime, v.title from videos v join access a on v.id = a.vidId join vid_pl vp on v.id = vp.vidId join playlists pl on vp.plId = pl.id where pl.userId = {user.id} and v.deleted = 0 and pl.handle = '{handle}' order by v.id desc"
    ui1 = db.query(final) | ~apply(lambda i,vi,cId,ve,te,se,dur,cls,ar,ha,vt,ti: [i,vi,
            'none' if cId is None else ('error' if isinstance(cId, str) else cId),
            'none' if ve is None else ('error' if ve else 'yes'),
            'none' if te is None else ('error' if te else 'yes'),
            'none' if se is None or se == '0' else ('error' if se else 'yes'),
            dur,cls,ar,ha,vt | (tryout() | toIso() | op().replace(*"T ")),ti])\
        | deref() | (toJsFunc("term", ("archived", ["true", "false", "both"], "false")) | grep("${term}") | filt("x if archived == 'true' else ((not x) if archived == 'false' else true)", 8)\
            | (shape(0) | aS('f"#rows: {x}"')) & viz.Table(["id", "vidId", "chatId", "hasVid", "hasTrans", "hasSound", "duration", "cleaned", "archived", "handle", "vidTime", "title"], onclickFName=f"vid_select", selectable=True, ondeleteFName="vid_delete", sortF=True, height=400)) | op().interface() | toHtml()
    return ui1.replace("<select id=", "<select class='select' id=")

def fragment_channels(guardRes):
    pre = init._jsDAuto(); iconF = "#e6edf3" if guardRes["darkmode"] else "#333"; user = db["users"][guardRes["userId"]]
    res = db.query("""select c.id, sum(a.archived), sum(coalesce(a.chatId, 0) > 0), count(a.id) from access a
                                  join videos v on a.vidId = v.id join channels c on v.channelId = c.id
                                  where a.userId = ? group by c.id""", user.id) | ~apply(lambda a,b,c,d: [[a,b], [a,c], [a,d]]) | T() | toDict().all() | aS(list)
    d1, d2, d3 = res if len(res) else [{}, {}, {}]
    ui1 = db.query(f"select c.id, c.provider, c.handle, c.fullscanErr, c.id, c.id, c.id from channels c join subs s on c.id = s.channelId where s.userId = ?", user.id)\
        | lookup(d1, 4, fill="(no data)") | lookup(d2, 5, fill="(no data)") | lookup(d3, 6, fill="(no data)") | apply(lambda arr: [*arr, round(arr[4]/arr[5]*100) if not isinstance(arr[5], str) and arr[5] > 0 else -1, round(arr[5]/arr[6]*100) if not isinstance(arr[6], str) and arr[6] > 0 else -1])\
        | apply(lambda x: 'null' if x is None else ("yes" if x == "" else "error"), 3) | deref()\
        | (toJsFunc("term") | grep("${term}") | viz.Table(["id", "provider", "handle", "fullscanErr", "archived", "ready", "total", "%read", "%ready"], height=400, onclickFName=f"{pre}_select", selectable=True, sortF=True)) | op().interface() | toHtml(); return f"""
<div class="flex_row" style="gap: 12px">
    <div style="flex: 1; overflow-x: auto">
        <div style="display: flex; flex-direction: row; align-items: center; margin-bottom: 24px; margin-top: 12px">
            <h2>Channels</h2>
            <input id="{pre}_url" class="input input-bordered" placeholder="(channel url)" style="margin-left: 24px; margin-right: 8px" onkeydown="if(event.key == 'Enter') {pre}_new();" />
            <button class="btn btn-outline" style="padding: 8px; margin-right: 4px; display: block" onclick="{pre}_new()">{k1.Icon.add(fill=iconF)}</button></div>
        <div style="overflow-x: auto; width: 100%">{ui1}</div></div>
    <div id="{pre}_res" style="flex: 1; overflow-x: auto; padding-top: 4px"></div></div>
<script>function {pre}_select(row, i, e) {{ dynamicLoad("#{pre}_res", `/fragment/channel/${{row[0]}}?token={guardRes['token']}`); channel_sel.value = row[2]; channel_sel.oninput(); }}
async function {pre}_new() {{ await wrapToastReq(fetchPost(`/api/channel/new?token={guardRes['token']}`, {{ url: {pre}_url.value.trim() }})); {pre}_url.value = ""; {pre}_url.focus(); }}</script>"""

@app.route("/api/channel/new", methods=["POST"], guard=tokenGuard)
def api_channel_new(js, guardRes):
    url = js["url"]; userId = guardRes["userId"]; handle = None
    if "youtube.com/" in url:
        if url.startswith("https://www.youtube.com/@"): handle = "@" + url.split("https://www.youtube.com/@")[1].split("/")[0]
        else: web.toast_error(f"Invalid youtube channel link")
    if handle is None: web.toast_error("Provider not found")
    channel = db["channels"].lookup(handle=handle)
    if channel is None: channel = db["channels"].insert(provider="yt", handle=handle, name="", partscanTime=0)
    sub = db["subs"].lookup(channelId=channel.id, userId=userId)
    if sub: web.toast_error("Channel added before!")
    db["subs"].insert(channelId=channel.id, userId=userId); return "ok"

@app.route("/fragment/channel/<int:channelId>", guard=tokenGuard)
def fragment_channel(channelId, guardRes): pre = init._jsDAuto(); channel = db["channels"][channelId]; return f"""<h3><a href='https://www.youtube.com/{channel.handle}/videos' target='_blank'>Channel {channel.handle}</a></h3>
<div style="display: flex; flex-direction: row; gap: 8px">
    <button class="btn btn-outline" onclick="wrapToastReq(fetch(`/api/channel/{channel.id}/fullScan/0?token={guardRes['token']}`))"  title="Scan first page only, done every midnight">Partial scan</button>
    <button class="btn btn-outline" onclick="wrapToastReq(fetch(`/api/channel/{channel.id}/fullScan/20?token={guardRes['token']}`))" title="Scans 20 scrolls, about 630 videos, done first time channel added">Full scan</button>
    <button class="btn btn-outline" onclick="wrapToastReq(fetch(`/api/channel/{channel.id}/fullScan/100?token={guardRes['token']}`))" title="Scans 100 scrolls, about 3k videos">Fucking deep scan</button>
    <button class="btn btn-outline" onclick="wrapToastReq(fetch(`/api/channel/{channel.id}/clearError?token={guardRes['token']}`))">Clear error</button></div>
<textarea id="{pre}_err" class="textarea" style="height: 400px; width: 100%; margin-top: 12px; {'' if guardRes['darkmode'] else 'border: #ddd 1px solid;'}"></textarea><script>{pre}_err.value = {json.dumps(channel.fullscanErr)}</script>"""

@app.route("/api/channel/<int:channelId>/clearError", guard=tokenGuard)
def api_channel_clearError(channelId): db["channels"][channelId].fullscanErr = None; return "ok"

# ------------------------------ playlists

def fragment_playlists(guardRes):
    pre = init._jsDAuto(); iconF = "#e6edf3" if guardRes["darkmode"] else "#333"; user = db["users"][guardRes["userId"]]
    ui1 = db.query("select id, name, lastScan, handle from playlists where userId = ?", user.id) | apply(toIso() | op().replace(*"T "), 2) | deref() | (toJsFunc("term") | grep("${term}") | viz.Table(["id", "name", "lastScan", "handle"], onclickFName=f"{pre}_select", selectable=True)) | op().interface() | toHtml()
    return f"""
<div class="flex_row" style="gap: 12px">
    <div style="flex: 1; overflow-x: auto">
        <div style="display: flex; flex-direction: row; align-items: center; margin-bottom: 24px; margin-top: 12px">
            <h2>Playlists</h2>
            <input id="{pre}_url" class="input input-bordered" placeholder="(playlist url)" style="margin-left: 24px; margin-right: 8px" onkeydown="if(event.key == 'Enter') {pre}_new();" />
            <button class="btn btn-outline" style="padding: 8px; margin-right: 4px; display: block" onclick="{pre}_new()">{k1.Icon.add(fill=iconF)}</button></div>
        <div style="overflow-x: auto; width: 100%">{ui1}</div></div>
    <div id="{pre}_res" style="flex: 1; overflow-x: auto; padding-top: 4px"></div></div>
<script>function {pre}_select(row, i, e) {{ dynamicLoad("#{pre}_res", `/fragment/playlist/${{row[0]}}?token={guardRes['token']}`); channel_sel.value = row[3]; channel_sel.oninput(); }}
async function {pre}_new() {{ await wrapToastReq(fetchPost(`/api/playlist/new?token={guardRes['token']}`, {{ url: {pre}_url.value.trim() }})); {pre}_url.value = ""; {pre}_url.focus(); }}</script>"""

@app.route("/api/playlist/new", methods=["POST"], guard=tokenGuard)
def api_playlist_new(js, guardRes):
    user = db["users"][guardRes["userId"]]; url = js["url"]
    if not url.startswith("https://www.youtube.com/playlist?list="): web.toast_error("Invalid playlist url format. Expected to start with 'https://www.youtube.com/playlist?list='")
    handle = url.split("https://www.youtube.com/playlist?list=")[1].split("&")[0]
    db["playlists"].insert(handle=handle, lastScan=0, userId=user.id); return "ok"

@app.route("/fragment/playlist/<int:playlistId>", guard=tokenGuard)
def fragment_playlist(playlistId, guardRes): pre = init._jsDAuto(); playlist = db["playlists"][playlistId]; return f"""<h3><a href='https://www.youtube.com/playlist?list={playlist.handle}' target='_blank'>Playlist {playlist.name}</a></h3>
<div style="display: flex; flex-direction: row; gap: 8px">
    <button class="btn btn-outline" onclick="wrapToastReq(fetch(`/api/playlist/{playlist.id}/scan?token={guardRes['token']}`))"  title="Scan the playlist, does this automatically every day">Scan</button></div>"""

@app.route("/api/playlist/<int:playlistId>/scan", guard=tokenGuard)
def api_playlist_fullScan(playlistId): db["playlists"][playlistId].lastScan = 0; return "ok"

# ------------------------------ video serving functions

@app.route('/raw/vid/<int:vidId>', guard=vidGuard)
def serve_video_streaming(vidId, request):
    vid = db["videos"][vidId]; filepath = f"vids/{vid.vidId}"
    if not os.path.exists(filepath): return 'File not found', 404, {"Content-Type": "text/plain"}
    file_size = os.path.getsize(filepath); range_header = request.headers.get('Range')
    if range_header:
        try:
            range_spec = range_header.replace('bytes=', '').strip()
            if '-' in range_spec: start, end = range_spec.split('-'); start = int(start) if start else 0; end = int(end) if end else file_size - 1
            else: return 'Invalid Range header', 416, {"Content-Type": 'text/plain'}
        except ValueError: return 'Invalid Range header', 416, {"Content-Type": 'text/plain'}
        content_length = end - start + 1
        def generate():
            with open(filepath, 'rb') as f:
                f.seek(start)
                while True:
                    chunk = f.read(8192)
                    if not chunk: break
                    yield chunk
        return generate(), 206, {"Content-Type": 'video/mp4', "Content-Length": str(content_length), "Content-Range": f'bytes {start}-{end}/{file_size}', "Accept-Ranges": "bytes"}
    else:
        with open(filepath, "rb") as f: return f.read()


@app.route("/api/vid/<vidId>/transcript")
@app.route("/api/vid/<vidId>/transcript/<format>")
def api_vid_transcript(vidId, format="text"):
    vid = db["videos"].lookup(vidId=vidId); postamble = "[Transcript over. If you see there are no content, there literally is no content, may be because it's just music or a silent video!]\n"
    if vid is None: web.notFound()
    if vid.transErr != "": web.notFound()
    if format == "vtt": return vid.trans + postamble
    return vid.trans.split("\n") | ~head(3) | batched(3) | item().all() | join("\n") + postamble

@app.route("/api/vid/<int:vidId>/delete", guard=vidGuard)
def api_vid_delete(vidId):
    vid = db["videos"][vidId]; vid.deleted = True; return "ok" # below are the old implementation that actually deletes the video, but kinda want to phase out that mechanism
    chatIds = db.query(f"select chatId from access where vidId = {vidId} and chatId is not null") | cut(0) | aS(list)
    if len(chatIds): res = sendAiServer(1, {"cmd": "deleteChat", "chatIds": chatIds}) # deletes old chat from ai server, to prevent clogging things up
    db.query(f"delete from access where vidId = {vidId}") # delete from access
    None | cmd(f"rm vids/{vid.vidId}") | ignore() # delete from fs
    del db["videos"][vidId]; return "ok" # delete from videos

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
    if vid.transErr == "": transTag = f"""<textarea class="textarea textarea-bordered" style="width: 100%; height: 480px">{vid.trans}</textarea>"""
    if isinstance(access.chatId, int): chatTag = f" - <a href='{aiServer}/schedules/{user.scheduleId}/{access.chatId}' target='_blank' style='color: blue'>Summary</a>"
    return f"""<style>#{pre}_main {{ flex-direction: row; }} @media (max-width: 800px) {{ #{pre}_main {{ flex-direction: column }} }}</style>
<h2><a href="{vid.url}" target="_blank">{vid.title}</a>{chatTag}</h2>
<div id="{pre}_main" style="display: flex; gap: 12px">
    <div style="flex: 1">{transTag}</div>
    <div id="vidHolder" style="flex: 1; display: grid; grid-template-columns: min-content auto; height: min-content; row-gap: 8px; column-gap: 8px; align-items: center">
        <button class="btn" onclick="vidHolder.style.display = 'block'; vidHolder.style.height = '480px'; dynamicLoad('#vidHolder', '/fragment/vid/{vid.id}?token={guardRes['token']}')">Load video</button><div></div>
        <button class="btn" onclick="wrapToastReq(fetch('/api/vid/{vid.id}/clear/vidErr?token={guardRes['token']}'))"  >Clear vidErr  </button><div id="{pre}_1"></div>
        <button class="btn" onclick="wrapToastReq(fetch('/api/vid/{vid.id}/clear/transErr?token={guardRes['token']}'))">Clear transErr</button><div id="{pre}_2"></div>
        <button class="btn" onclick="wrapToastReq(fetch('/api/vid/{vid.id}/clear/chatId?token={guardRes['token']}'))"  >Clear chatId  </button><div id="{pre}_3"></div>
        <button class="btn" onclick="wrapToastReq(fetch('/api/vid/{vid.id}/clear/title?token={guardRes['token']}'))"   >Clear title   </button><div id="{pre}_4"></div>
        <button class="btn" onclick="wrapToastReq(fetch('/api/vid/{vid.id}/clear/soundErr?token={guardRes['token']}'))">Clear soundErr</button><div id="{pre}_5"></div>
        <button class="btn" onclick="wrapToastReq(fetch('/api/vid/{vid.id}/clear/duration?token={guardRes['token']}'))">Clear duration</button><div id="{pre}_6"></div>
        <button class="btn" onclick="wrapToastReq(fetch('/api/vid/{vid.id}/clear/retain?token={guardRes['token']}'))"  >Retain        </button><div>Retain video, dont delete to save space</div>
    </div></div>
<script>{pre}_1.innerHTML = {json.dumps(vid.vidErr)}; {pre}_2.innerHTML = {json.dumps(vid.transErr)};
{pre}_3.innerHTML = {json.dumps(access.chatId)}; {pre}_4.innerHTML = {json.dumps(vid.title)}; {pre}_5.innerHTML = {json.dumps(vid.soundErr)}; {pre}_6.innerHTML = {json.dumps(vid.duration)};</script>"""

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
    if resource == "soundErr": vid.soundErr = None
    if resource == "duration": vid.duration = None
    if resource == "retain":   vid.retain = True
    return "ok"

transferSgn1 = """
import time, threading
def inner(): time.sleep(20); changeModel('smart')
def func(): threading.Thread(target=inner, daemon=True).start()
"""

@toolCatchErr
def ytTranscript(vidId:str, env) -> str:
    """Get transcript of specific youtube video"""
    yield {"type": "status", "content": "Fetching transcript"}
    return {"resultType": "str", "result": api_vid_transcript(vidId, "vtt"), "func": transferSgn1}

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
    for userId, _time in db.query("select a.userId, sum(v.duration) from videos v join access a on v.id = a.vidId group by a.userId"):
        s += f'yt_time_total{{userId="{userId}"}} {_time}\n'
    for userId, nChats in db.query("select a.userId, count(v.duration) from videos v join access a on v.id = a.vidId where a.archived = 1 group by a.userId"):
        s += f'yt_archivedChats_count{{userId="{userId}"}} {nChats}\n'
    for userId, nChats in db.query("select a.userId, count(v.duration) from videos v join access a on v.id = a.vidId group by a.userId"):
        s += f'yt_chats_count{{userId="{userId}"}} {nChats}\n'
    return s

sql.lite_flask(app, guard=adminGuard); k1.logErr.flask(app, guard=adminGuard); k1.cron.flask(app, guard=adminGuard)

app.run(host="0.0.0.0", port=5008) # same as normal flask code








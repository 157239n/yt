from k1lib.imports import *
import magic
from urllib.parse import urlparse, parse_qs
from schemaParser import *
from dbs import *

settings.timezone = "Asia/Hanoi"
yt_dlp = "/home/kelvin/envs/torch/bin/yt-dlp --js-runtimes 'deno:/home/kelvin/.deno/bin/deno' "
aiServer = "https://ai.aigu.vn"
ytServer = "https://yt.aigu.vn"

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


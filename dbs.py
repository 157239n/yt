from k1lib.imports import *

__all__ = ["db"]

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
    vidTime     INTEGER, -- what time does the vid finishes downloading?
    soundErr    TEXT,    -- if convert to sound file not successful, contains the error and traceback. If successful, an empty string, if not executed, null
    deleted     BOOL     -- mark the video as being deleted, but don't actually delete it, due to channel scanning readding private videos automatically
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
    provider     TEXT,    -- only yt for now
    handle       TEXT,    -- channel names, starts with @
    name         TEXT,    -- channel display name
    fullscanErr  TEXT,    -- if successful, an empty string, if not executed, null, else error
    fullscanTime INTEGER, -- time when finishes last full scan
    partscanTime INTEGER  -- last partial scan time
);""")

db.query("""CREATE TABLE IF NOT EXISTS subs ( -- track which user is subscribed to which channel
    id          INTEGER primary key autoincrement,
    userId      INTEGER,
    channelId   INTEGER
);""")

db.query("""CREATE TABLE IF NOT EXISTS playlists ( -- what playlists to scrape automatically. Currently only applies to user 1, since it's too much of a hassle to make this user-agnostic!
    id          INTEGER primary key autoincrement,
    name        TEXT,
    handle      TEXT,
    lastScan    INTEGER,
    userId      INTEGER,
    compileErr  TEXT     -- set to null if want to recompile the playlist's [title].mp3 list
);""")

db.query("""CREATE TABLE IF NOT EXISTS vid_pl ( -- joins videos and playlists
    id          INTEGER primary key autoincrement,
    vidId       INTEGER,
    plId        INTEGER,
    idx         INTEGER  -- order within the playlist
);""")
db.query("CREATE INDEX IF NOT EXISTS vid_pl_vidId ON vid_pl (vidId);")
db.query("CREATE INDEX IF NOT EXISTS vid_pl_plId ON vid_pl (plId);")





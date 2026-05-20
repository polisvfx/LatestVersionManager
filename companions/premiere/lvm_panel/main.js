// LVM Sync Versions panel — main JS.
//
// Polls %APPDATA%/LVM/triggers/ (or platform equivalent) for trigger
// JSON files written by the LVM desktop app. When a trigger appears,
// runs lvmRestoreVersions() in host.jsx via CSInterface, then deletes
// the trigger.
//
// Also writes a heartbeat file every ~10s so LVM knows the panel is
// running and can enable its "Sync Names → Premiere" button.
//
// Node.js (cep_node) provides fs / path / os, enabled in manifest.xml
// via <Parameter>--enable-nodejs</Parameter>.

(function () {
    "use strict";

    var POLL_INTERVAL_MS = 1000;
    var HEARTBEAT_INTERVAL_MS = 10000;
    var TRIGGER_MAX_AGE_MS = 5 * 60 * 1000;  // ignore triggers older than 5 minutes

    var cs = new CSInterface();
    var statusEl = document.getElementById("status");
    var btn = document.getElementById("sync-now");
    var logEl = document.getElementById("log");

    // Node bindings (CEF runs node when --enable-nodejs is set).
    var fs, path, os;
    try {
        fs = cep_node.require("fs");
        path = cep_node.require("path");
        os = cep_node.require("os");
    } catch (e) {
        setStatus("error", "Node.js unavailable in this Premiere version. " +
                  "Panel requires CEP 9+ (Premiere Pro 2020 / 14.0+).");
        btn.disabled = true;
        return;
    }

    var lvmDir = computeLvmDir();
    var triggerDir = path.join(lvmDir, "triggers");
    var heartbeatDir = path.join(lvmDir, "heartbeat");
    var heartbeatPath = path.join(heartbeatDir, "premiere.json");

    var seenTriggers = {};
    var renaming = false;

    ensureDir(triggerDir);
    ensureDir(heartbeatDir);
    writeHeartbeat();

    setStatus("ready", "Watching " + triggerDir);
    setInterval(writeHeartbeat, HEARTBEAT_INTERVAL_MS);
    setInterval(pollTriggers, POLL_INTERVAL_MS);

    btn.addEventListener("click", function () {
        runRename("manual button click");
    });

    // ----- helpers -----

    function computeLvmDir() {
        // Mirror nle_bridge.lvm_data_dir() exactly so triggers and
        // heartbeat live where LVM expects them.
        var platform = os.platform();
        var home = os.homedir();
        if (platform === "win32") {
            var appdata = process.env.APPDATA;
            if (!appdata) appdata = path.join(home, "AppData", "Roaming");
            return path.join(appdata, "LVM");
        }
        if (platform === "darwin") {
            return path.join(home, "Library", "Application Support", "LVM");
        }
        // linux + others
        var xdg = process.env.XDG_DATA_HOME;
        if (xdg) return path.join(xdg, "LVM");
        return path.join(home, ".local", "share", "LVM");
    }

    function ensureDir(dir) {
        try {
            fs.mkdirSync(dir, { recursive: true });
        } catch (e) {
            // existsSync race / permission denied; surfaced later if it matters
        }
    }

    function setStatus(klass, msg) {
        statusEl.className = klass || "";
        statusEl.textContent = msg;
    }

    function appendLog(level, msg) {
        var span = document.createElement("div");
        span.className = "log-" + level;
        var stamp = new Date().toLocaleTimeString();
        span.textContent = "[" + stamp + "] " + msg;
        logEl.appendChild(span);
        logEl.scrollTop = logEl.scrollHeight;
        // Keep the log bounded.
        while (logEl.childNodes.length > 200) {
            logEl.removeChild(logEl.firstChild);
        }
    }

    function writeHeartbeat() {
        try {
            var payload = JSON.stringify({
                updated_at: new Date().toISOString(),
                pid: process.pid,
                version: "1.0.0"
            });
            // Atomic write: tmp + rename.
            var tmp = heartbeatPath + ".tmp";
            fs.writeFileSync(tmp, payload, "utf8");
            fs.renameSync(tmp, heartbeatPath);
        } catch (e) {
            // Heartbeat isn't critical to the panel's own function.
        }
    }

    function pollTriggers() {
        if (renaming) return;
        var entries;
        try {
            entries = fs.readdirSync(triggerDir);
        } catch (e) {
            return;
        }
        var now = Date.now();
        for (var i = 0; i < entries.length; i++) {
            var name = entries[i];
            if (name.indexOf(".json") < 0) continue;
            if (name.indexOf(".tmp") >= 0) continue;
            var full = path.join(triggerDir, name);
            if (seenTriggers[full]) continue;

            var stat;
            try {
                stat = fs.statSync(full);
            } catch (e) {
                continue;
            }
            if (now - stat.mtimeMs > TRIGGER_MAX_AGE_MS) {
                // Stale — delete and ignore.
                tryDelete(full);
                continue;
            }
            seenTriggers[full] = true;
            handleTrigger(full);
            return;  // one at a time
        }
    }

    function handleTrigger(triggerPath) {
        var parsed = null;
        try {
            var raw = fs.readFileSync(triggerPath, "utf8");
            parsed = JSON.parse(raw);
        } catch (e) {
            appendLog("warn", "Trigger " + path.basename(triggerPath) +
                      " was unreadable: " + e);
        }
        appendLog("info", "Trigger received: " + path.basename(triggerPath));

        // Schema v2+: trigger carries the renames inline. One ExtendScript
        // round-trip applies the whole batch. Older payloads (no
        // schema_version, or v1) fall back to the full sidecar scan so
        // panels that pre-date this change keep working.
        var schema = parsed && parsed.schema_version;
        var renames = parsed && parsed.renames;
        if (schema && schema >= 2 && renames && renames.length !== undefined) {
            runBatchRename(renames, "trigger " + path.basename(triggerPath),
                function () { tryDelete(triggerPath); });
        } else {
            runRename("trigger " + path.basename(triggerPath), function () {
                tryDelete(triggerPath);
            });
        }
    }

    function tryDelete(p) {
        try { fs.unlinkSync(p); } catch (e) { /* ignore */ }
    }

    function runRename(reason, done) {
        if (renaming) {
            appendLog("warn", "Already running, ignoring: " + reason);
            if (done) done();
            return;
        }
        renaming = true;
        btn.disabled = true;
        setStatus("busy", "Renaming clips… (" + reason + ")");

        cs.evalScript("lvmRestoreVersions()", function (result) {
            renaming = false;
            btn.disabled = false;

            var stats = parseStats(result);
            if (stats && stats.ok) {
                setStatus("ready", "Done. Watching " + triggerDir);
                var tl = (typeof stats.timeline_renamed === "number")
                    ? (", timeline " + stats.timeline_renamed) : "";
                appendLog("info",
                    "Renamed " + stats.renamed +
                    ", up-to-date " + stats.idempotent +
                    ", no-match " + stats.no_match + tl +
                    ", errors " + stats.errors);
            } else if (stats) {
                setStatus("error", stats.error || "Failed (see log)");
                appendLog("error", stats.error || "Rename failed");
            } else {
                setStatus("error", "ExtendScript returned: " + result);
                appendLog("error", "Unparseable host result: " + result);
            }
            if (done) done();
        });
    }

    function runBatchRename(renames, reason, done) {
        if (renaming) {
            appendLog("warn", "Already running, ignoring: " + reason);
            if (done) done();
            return;
        }
        renaming = true;
        btn.disabled = true;
        setStatus("busy", "Renaming " + renames.length + " clip(s)… (" + reason + ")");

        // ExtendScript wants a single string-quoted arg. JSON-encode
        // twice so the inner JSON survives evalScript's parser.
        var payload = JSON.stringify(JSON.stringify(renames));
        cs.evalScript("lvmRenameBatch(" + payload + ")", function (result) {
            renaming = false;
            btn.disabled = false;

            var stats = parseStats(result);
            if (stats && stats.ok) {
                setStatus("ready", "Done. Watching " + triggerDir);
                var req = (typeof stats.requested === "number")
                    ? ("/" + stats.requested) : "";
                var tl = (typeof stats.timeline_renamed === "number")
                    ? (", timeline " + stats.timeline_renamed) : "";
                appendLog("info",
                    "Batch: renamed " + stats.renamed + req +
                    ", up-to-date " + stats.idempotent +
                    ", no-match " + stats.no_match + tl +
                    ", errors " + stats.errors);
            } else if (stats) {
                setStatus("error", stats.error || "Failed (see log)");
                appendLog("error", stats.error || "Batch rename failed");
            } else {
                setStatus("error", "ExtendScript returned: " + result);
                appendLog("error", "Unparseable host result: " + result);
            }
            if (done) done();
        });
    }

    function parseStats(result) {
        if (!result) return null;
        // host.jsx wraps its return in a recognisable JSON envelope.
        var marker = "__LVM_RESULT__";
        var idx = result.indexOf(marker);
        if (idx < 0) return null;
        var json = result.substring(idx + marker.length);
        try {
            return JSON.parse(json);
        } catch (e) {
            return null;
        }
    }
})();

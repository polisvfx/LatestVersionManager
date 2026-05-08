// LVM Sync Versions — host ExtendScript for the CEP panel.
//
// Exposes lvmRestoreVersions() globally so main.js can call it via
// CSInterface.evalScript(). The function returns a string with a marker
// (__LVM_RESULT__) followed by a JSON envelope main.js parses.
//
// Logic mirrors companions/premiere/lvm_restore_versions.jsx (the
// standalone script for File -> Scripts -> Run Script File). Kept as a
// separate file rather than #include so the standalone version remains
// installable without the rest of the panel.

#target premierepro

var LVM = LVM || {};

(function (ns) {
    var SIDECAR_PREFIX = ".latest_history";
    var SIDECAR_SUFFIX = ".json";

    var VERSION_RE = /[._\-]v\d+/i;
    var FRAME_EXT_RE = /([._])(\d+)\.(\w+)$/;

    function readFileText(path) {
        var f = new File(path);
        if (!f.exists) return null;
        if (!f.open("r")) return null;
        f.encoding = "UTF-8";
        try {
            return f.read();
        } finally {
            f.close();
        }
    }

    function parseJSON(text) {
        if (text === null || text === undefined) return null;
        try {
            if (typeof JSON !== "undefined" && JSON.parse) {
                return JSON.parse(text);
            }
        } catch (e) {
            // fall through
        }
        try {
            if (/[^,:{}\[\]0-9.\-+Eaeflnr-u \n\r\t"]/.test(
                    text.replace(/"(\\.|[^"\\])*"/g, ""))) {
                return null;
            }
            return eval("(" + text + ")");
        } catch (e2) {
            return null;
        }
    }

    function stringifyJSON(obj) {
        if (typeof JSON !== "undefined" && JSON.stringify) {
            return JSON.stringify(obj);
        }
        // Minimal fallback for ExtendScript hosts without JSON polyfill.
        var parts = [];
        for (var k in obj) {
            if (!obj.hasOwnProperty(k)) continue;
            var v = obj[k];
            var s;
            if (typeof v === "number" || typeof v === "boolean") {
                s = String(v);
            } else if (v === null || v === undefined) {
                s = "null";
            } else {
                s = "\"" + String(v).replace(/\\/g, "\\\\").replace(/"/g, "\\\"") + "\"";
            }
            parts.push("\"" + k + "\":" + s);
        }
        return "{" + parts.join(",") + "}";
    }

    function readSidecar(path) {
        var txt = readFileText(path);
        if (!txt) return null;
        return parseJSON(txt);
    }

    function dirAndBaseFromPath(p) {
        if (!p) return { dir: "", base: "" };
        var norm = p.replace(/\\/g, "/");
        var idx = norm.lastIndexOf("/");
        if (idx < 0) return { dir: "", base: norm };
        return { dir: norm.substring(0, idx), base: norm.substring(idx + 1) };
    }

    function listSidecars(dir) {
        if (!dir) return [];
        var folder = new Folder(dir);
        if (!folder.exists) return [];
        var entries = folder.getFiles(function (f) {
            if (!(f instanceof File)) return false;
            var n = f.name;
            return n.length > SIDECAR_PREFIX.length + SIDECAR_SUFFIX.length &&
                   n.substring(0, SIDECAR_PREFIX.length) === SIDECAR_PREFIX &&
                   n.substring(n.length - SIDECAR_SUFFIX.length) === SIDECAR_SUFFIX;
        });
        var out = [];
        for (var i = 0; i < entries.length; i++) out.push(entries[i].fsName);
        return out;
    }

    function deriveStemFromSource(sourcePath) {
        if (!sourcePath) return "";
        var pieces = dirAndBaseFromPath(sourcePath);
        var base = pieces.base;
        var dot = base.lastIndexOf(".");
        var stem = dot > 0 ? base.substring(0, dot) : base;
        if (!VERSION_RE.test(stem)) return "";
        var stripped = stem.replace(VERSION_RE, "");
        stripped = stripped.replace(/([_.\-]){2,}/g, "$1");
        stripped = stripped.replace(/^[_.\-]+|[_.\-]+$/g, "");
        if (!stripped) return "";
        return stripped + "_latest";
    }

    function stemMatchesClip(stem, clipBasename) {
        if (!stem || !clipBasename) return false;
        var dot = clipBasename.lastIndexOf(".");
        if (dot > 0 && clipBasename.substring(0, dot) === stem) return true;
        if (clipBasename.substring(0, stem.length + 1) === stem + "." ||
            clipBasename.substring(0, stem.length + 1) === stem + "_") return true;
        return false;
    }

    function newDisplayName(sourcePath, clipBasename) {
        if (!sourcePath) return "";
        var sourceBase = dirAndBaseFromPath(sourcePath).base;
        if (!sourceBase) return "";
        var m = clipBasename.match(FRAME_EXT_RE);
        if (m) {
            var sep = m[1], frame = m[2], ext = m[3];
            var dot = sourceBase.lastIndexOf(".");
            var sourceStem = dot > 0 ? sourceBase.substring(0, dot) : sourceBase;
            return sourceStem + sep + frame + "." + ext;
        }
        return sourceBase;
    }

    function matchSidecarToClip(clipPath) {
        var pieces = dirAndBaseFromPath(clipPath);
        if (!pieces.dir || !pieces.base) return null;
        var sidecars = listSidecars(pieces.dir);
        sidecars.sort();
        for (var i = 0; i < sidecars.length; i++) {
            var data = readSidecar(sidecars[i]);
            if (!data) continue;
            var cur = data.current;
            if (!cur) continue;
            var stem = cur.latest_basename || deriveStemFromSource(cur.source || "");
            if (stemMatchesClip(stem, pieces.base)) {
                return { sidecar: data, path: sidecars[i] };
            }
        }
        return null;
    }

    function isClip(item) {
        try {
            return item.type === ProjectItemType.CLIP;
        } catch (e) {
            return false;
        }
    }

    function walkProjectItems(item, visit) {
        if (!item) return;
        if (isClip(item)) {
            visit(item);
            return;
        }
        var kids = null;
        try { kids = item.children; } catch (e) { kids = null; }
        if (!kids) return;
        var n = 0;
        try { n = kids.numItems; } catch (e2) { n = 0; }
        for (var i = 0; i < n; i++) {
            try { walkProjectItems(kids[i], visit); } catch (e3) { /* skip */ }
        }
    }

    function renameOnce() {
        if (!app.project) {
            return { ok: false, error: "No project is open." };
        }
        var renamed = 0;
        var skippedMatch = 0;
        var skippedIdempotent = 0;
        var errors = 0;

        walkProjectItems(app.project.rootItem, function (clip) {
            var clipPath = "";
            try { clipPath = clip.getMediaPath() || ""; } catch (e) { clipPath = ""; }
            if (!clipPath) { skippedMatch++; return; }

            var match = matchSidecarToClip(clipPath);
            if (!match) { skippedMatch++; return; }

            var clipBase = dirAndBaseFromPath(clipPath).base;
            var newName = newDisplayName(match.sidecar.current.source || "", clipBase);
            if (!newName) { skippedMatch++; return; }

            var currentName = "";
            try { currentName = clip.name || ""; } catch (e2) { currentName = ""; }
            if (currentName === newName) { skippedIdempotent++; return; }

            try {
                clip.name = newName;
                renamed++;
            } catch (e3) {
                errors++;
            }
        });

        return {
            ok: true,
            renamed: renamed,
            idempotent: skippedIdempotent,
            no_match: skippedMatch,
            errors: errors
        };
    }

    ns.renameOnce = renameOnce;
    ns.stringifyJSON = stringifyJSON;
})(LVM);

// Top-level callable for CSInterface.evalScript. Returns a string with
// a known marker so main.js can parse stats reliably regardless of any
// stdout the host might also emit.
function lvmRestoreVersions() {
    var result;
    try {
        result = LVM.renameOnce();
    } catch (e) {
        result = { ok: false, error: "Host raised: " + e };
    }
    return "__LVM_RESULT__" + LVM.stringifyJSON(result);
}

// LVM Restore Versions — Adobe Premiere Pro companion script.
//
// Walks the open project's media pool and renames each clip's display
// name to the source version recorded in the LVM sidecar
// (.latest_history*.json) found next to the clip on disk.
//
// The on-disk file is not touched — only the ProjectItem.name in
// Premiere's project panel. Re-running is idempotent.
//
// Run via: File -> Scripts -> Run Script File -> pick this .jsx
//
// Tested against Premiere Pro 2024 (ExtendScript). Supported by Adobe
// through September 2026; a UXP port is the long-term path.

#target premierepro

(function () {
    var SIDECAR_PREFIX = ".latest_history";
    var SIDECAR_SUFFIX = ".json";

    // ----- minimal JSON parser (ExtendScript lacks a guaranteed JSON global) -----

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
        // ExtendScript bundles the ES5 JSON polyfill since CS6, but older
        // hosts and panel sandboxes can be inconsistent. Try the native
        // path first, then fall back to eval in a sanitized scope.
        try {
            if (typeof JSON !== "undefined" && JSON.parse) {
                return JSON.parse(text);
            }
        } catch (e) {
            // fall through to eval-based fallback
        }
        try {
            // eval-based parse: wrap in parens so object literals parse,
            // and reject anything that isn't pure data syntax to limit risk.
            if (/[^,:{}\[\]0-9.\-+Eaeflnr-u \n\r\t"]/.test(
                    text.replace(/"(\\.|[^"\\])*"/g, ""))) {
                return null;
            }
            return eval("(" + text + ")");
        } catch (e2) {
            return null;
        }
    }

    function readSidecar(path) {
        var txt = readFileText(path);
        if (!txt) return null;
        return parseJSON(txt);
    }

    // ----- path helpers (ExtendScript File.fsName uses platform separators) -----

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
        // ">=" so the bare ".latest_history.json" (exactly prefix+suffix
        // chars long) isn't filtered out alongside the namespaced
        // ".latest_history_<name>.json" siblings.
        var entries = folder.getFiles(function (f) {
            if (!(f instanceof File)) return false;
            var n = f.name;
            return n.length >= SIDECAR_PREFIX.length + SIDECAR_SUFFIX.length &&
                   n.substring(0, SIDECAR_PREFIX.length) === SIDECAR_PREFIX &&
                   n.substring(n.length - SIDECAR_SUFFIX.length) === SIDECAR_SUFFIX;
        });
        var out = [];
        for (var i = 0; i < entries.length; i++) out.push(entries[i].fsName);
        return out;
    }

    // ----- stem matching (mirrors the Resolve script logic) -----

    var VERSION_RE = /[._\-]v\d+/i;
    var FRAME_EXT_RE = /([._])(\d+)\.(\w+)$/;

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

    function newDisplayName(sourcePath, clipBasename, cur) {
        cur = cur || {};
        var m = clipBasename.match(FRAME_EXT_RE);
        var stem = cur.nle_display_stem || "";

        if (stem) {
            var includeFrame = !!cur.nle_display_include_frame;
            var includeExt = !!cur.nle_display_include_extension;
            var name = stem;
            if (m && includeFrame) {
                name = name + m[1] + m[2];
            }
            if (includeExt) {
                var ext;
                if (m) {
                    ext = m[3];
                } else {
                    var dotI = clipBasename.lastIndexOf(".");
                    ext = dotI > 0 ? clipBasename.substring(dotI + 1) : "";
                }
                if (ext) name = name + "." + ext;
            }
            return name;
        }

        // Legacy fallback for sidecars written by older LVM versions.
        if (!sourcePath) return "";
        var sourceBase = dirAndBaseFromPath(sourcePath).base;
        if (!sourceBase) return "";
        if (m) {
            var sep = m[1], frame = m[2], ext2 = m[3];
            var dot = sourceBase.lastIndexOf(".");
            var sourceStem = dot > 0 ? sourceBase.substring(0, dot) : sourceBase;
            return sourceStem + sep + frame + "." + ext2;
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

    // ----- project tree walk -----

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
        // Bins expose a children collection
        var kids = null;
        try { kids = item.children; } catch (e) { kids = null; }
        if (!kids) return;
        var n = 0;
        try { n = kids.numItems; } catch (e2) { n = 0; }
        for (var i = 0; i < n; i++) {
            try { walkProjectItems(kids[i], visit); } catch (e3) { /* skip */ }
        }
    }

    // ----- main -----

    // ----- timeline track-item rename helpers -----
    //
    // ProjectItem.name only updates the Project panel. Timeline clips
    // (TrackItem) carry their own writable name field that was snapshot-
    // copied at placement time, so a project-item rename leaves stale
    // "_latest" labels in every sequence. Walk the sequences once and
    // rename matching track items.

    function projectItemKey(item) {
        try {
            var id = item.nodeId;
            if (id) return String(id);
        } catch (e) { /* fall through */ }
        try {
            return "path:" + (item.getMediaPath() || "");
        } catch (e2) {
            return "";
        }
    }

    function renameTrackItemsForRenames(renames) {
        var renamed = 0;
        var errors = 0;
        var seqs;
        try { seqs = app.project.sequences; } catch (e) { return { renamed: 0, errors: 0 }; }
        var nseq = 0;
        try { nseq = seqs.numSequences; } catch (e2) { nseq = 0; }

        for (var s = 0; s < nseq; s++) {
            var seq;
            try { seq = seqs[s]; } catch (e3) { continue; }
            if (!seq) continue;

            var trackGroups = [];
            try { if (seq.videoTracks) trackGroups.push(seq.videoTracks); } catch (e4) {}
            try { if (seq.audioTracks) trackGroups.push(seq.audioTracks); } catch (e5) {}

            for (var g = 0; g < trackGroups.length; g++) {
                var tracks = trackGroups[g];
                var ntracks = 0;
                try { ntracks = tracks.numTracks; } catch (e6) { ntracks = 0; }
                for (var t = 0; t < ntracks; t++) {
                    var track;
                    try { track = tracks[t]; } catch (e7) { continue; }
                    if (!track) continue;
                    var clips;
                    try { clips = track.clips; } catch (e8) { continue; }
                    if (!clips) continue;
                    var nclips = 0;
                    try { nclips = clips.numItems; } catch (e9) { nclips = 0; }

                    for (var c = 0; c < nclips; c++) {
                        var ti;
                        try { ti = clips[c]; } catch (e10) { continue; }
                        if (!ti) continue;

                        var src;
                        try { src = ti.projectItem; } catch (e11) { src = null; }
                        if (!src) continue;

                        var key = projectItemKey(src);
                        if (!key || !renames.hasOwnProperty(key)) continue;
                        var target = renames[key];

                        var current = "";
                        try { current = ti.name || ""; } catch (e12) { current = ""; }
                        if (current === target) continue;

                        try {
                            ti.name = target;
                            renamed++;
                        } catch (e13) {
                            errors++;
                        }
                    }
                }
            }
        }
        return { renamed: renamed, errors: errors };
    }

    if (!app.project) {
        alert("LVM Restore Versions: no project is open.");
        return;
    }

    var renamed = 0;
    var skippedMatch = 0;
    var skippedIdempotent = 0;
    var errors = 0;
    var log = [];
    var renames = {};

    walkProjectItems(app.project.rootItem, function (clip) {
        var clipPath = "";
        try { clipPath = clip.getMediaPath() || ""; } catch (e) { clipPath = ""; }
        if (!clipPath) { skippedMatch++; return; }

        var match = matchSidecarToClip(clipPath);
        if (!match) { skippedMatch++; return; }

        var clipBase = dirAndBaseFromPath(clipPath).base;
        var cur = match.sidecar.current || {};
        var newName = newDisplayName(cur.source || "", clipBase, cur);
        if (!newName) { skippedMatch++; return; }

        // Queue the timeline rename even when the project item is already
        // up-to-date — placed clips can drift from their source.
        var key = projectItemKey(clip);
        if (key) renames[key] = newName;

        var currentName = "";
        try { currentName = clip.name || ""; } catch (e2) { currentName = ""; }
        if (currentName === newName) { skippedIdempotent++; return; }

        try {
            clip.name = newName;
            renamed++;
            log.push(currentName + "  ->  " + newName);
        } catch (e3) {
            errors++;
            log.push("error: " + currentName + " -> " + newName + " (" + e3 + ")");
        }
    });

    var tlResult = renameTrackItemsForRenames(renames);

    var summary =
        "LVM Restore Versions\n\n" +
        "Renamed:             " + renamed + "\n" +
        "Already up to date:  " + skippedIdempotent + "\n" +
        "No sidecar match:    " + skippedMatch + "\n" +
        "Timeline items renamed: " + tlResult.renamed + "\n" +
        "Errors:              " + (errors + tlResult.errors);

    if (log.length > 0) {
        summary += "\n\n" + log.slice(0, 30).join("\n");
        if (log.length > 30) {
            summary += "\n... (" + (log.length - 30) + " more)";
        }
    }

    alert(summary);
})();

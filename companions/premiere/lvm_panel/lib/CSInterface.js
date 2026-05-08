/**
 * Minimal CSInterface shim for the LVM Sync Versions panel.
 *
 * The full Adobe-distributed CSInterface.js is ~1400 lines and exposes
 * dozens of APIs; we only need evalScript (to call host.jsx) and
 * getSystemPath (used as a fallback for resolving the trigger directory
 * when Node.js path lookups fail).
 *
 * Mirrors the public surface of the official library:
 *   https://github.com/Adobe-CEP/CEP-Resources
 *
 * Licence: MIT — see Adobe's CEP-Resources repository for the full
 * upstream copy and original copyright notice.
 */
(function (root) {
    "use strict";

    if (root.CSInterface) return;

    function CSInterface() {
        // No state; the host bridge is exposed as a global by the CEP runtime.
    }

    CSInterface.prototype.evalScript = function (script, callback) {
        if (typeof callback !== "function") {
            callback = function () {};
        }
        if (typeof window.__adobe_cep__ !== "undefined" &&
            window.__adobe_cep__ &&
            typeof window.__adobe_cep__.evalScript === "function") {
            window.__adobe_cep__.evalScript(script, callback);
            return;
        }
        // Fallback for non-CEP contexts (e.g. opened in a browser for
        // dev): reply asynchronously with an error sentinel so callers
        // don't deadlock waiting for a callback.
        setTimeout(function () {
            callback("EvalScript error: not running inside CEP host.");
        }, 0);
    };

    CSInterface.prototype.getSystemPath = function (pathType) {
        if (typeof window.__adobe_cep__ !== "undefined" &&
            window.__adobe_cep__ &&
            typeof window.__adobe_cep__.getSystemPath === "function") {
            return window.__adobe_cep__.getSystemPath(pathType);
        }
        return "";
    };

    // Common pathType constants for getSystemPath, mirroring Adobe's
    // SystemPath enum. Unused by the panel today but kept for parity.
    CSInterface.SystemPath = {
        USER_DATA: "userData",
        COMMON_FILES: "commonFiles",
        MY_DOCUMENTS: "myDocuments",
        APPLICATION: "application",
        EXTENSION: "extension",
        HOST_APPLICATION: "hostApplication"
    };

    root.CSInterface = CSInterface;
})(typeof window !== "undefined" ? window : this);

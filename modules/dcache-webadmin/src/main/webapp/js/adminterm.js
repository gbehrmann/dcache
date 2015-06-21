/**
 * Created by arossi on 5/12/15.
 */

var wsUri;
var websocket;
var term;
var rows;
var cols;

var SCROLL_INFO="Use control key + arrows or mouse wheel to scroll vertically.\r\n";
var SOCKET_DISCONNECT="Server has disconnected; to reconnect, refresh the page.";
var TERM_DISCONNECT="Terminal disconnected; to reconnect, refresh the page.";
var WARN_BEGIN="\r\n\x1b[33m";
var WARN_END="\x1b[m\r\n";

function init() {
    wsUri = document.getElementById("server").innerHTML;
    rows = document.getElementById("rows").innerHTML;
    cols = document.getElementById("cols").innerHTML;
    initTerminal();
    initWebSocket();
}

function initWebSocket() {
    websocket = new WebSocket(wsUri);
    websocket.onopen = function(evt) { onOpen(evt) };
    websocket.onclose = function(evt) { onClose(evt) };
    websocket.onmessage = function(evt) { onMessage(evt) };
    websocket.onerror = function(evt) { onError(evt) };
}

function initTerminal() {
    term = new Terminal({
        cols: this.cols,
        rows: this.rows,
        useStyle: true,
        useMouse: true,
        screenKeys: false,
        cursorBlink: true,
        convertEol: true,
        scrollback: 10000
    });

    term.on('data', function(data) {
        doSend(data);
    });

    term.on('title', function(title) {
        document.title = title;
    });

    term.open(document.getElementById("term"));

    window.addEventListener("unload", term.destroy, false);
};

function onOpen(evt) {
    console.log("CONNECTED");
    writeToTerminal(WARN_BEGIN + SCROLL_INFO + WARN_END);
}

function onClose(evt) {
    console.log("DISCONNECTED " + evt.data);
    writeToTerminal(WARN_BEGIN + SOCKET_DISCONNECT + WARN_END);
}

function onMessage(evt) {
    var msg = evt.data;
    console.log("RECEIVED '" + msg + "'");
    if (msg.substring(0, 15) == "TERMINAL_CLOSED") {
        msg = WARN_BEGIN + TERM_DISCONNECT + WARN_END;
    }
    writeToTerminal(msg);
}

function onError(evt) {
    console.error("ERROR " + evt.data);
}

function doSend(message) {
    console.log("SENT: " + message);
    websocket.send(message);
}

function writeToTerminal(message) {
    term.write(message);
}

window.addEventListener("load", init, false);
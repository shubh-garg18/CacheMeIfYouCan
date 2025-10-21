import * as net from "net";
import { RESPparser } from "./Utils/RESPparser.ts";
import { executeCommand } from "./commands";
import { getPending } from "./commands/lists";
import { RDBsetup } from "./Utils/RDBparser.ts";
import { getPendingReads } from "./commands/streams.ts";

console.log("Logs from your program will appear here!");

export const transactionQueue = new Map<net.Socket, string[][]>();

RDBsetup();

const server: net.Server = net.createServer((connection: net.Socket) => {
    connection.on("close", () => {
        // Clean up pending connections when client disconnects
        const pending = getPending();
        for (const listName in pending) {
            const idx = pending[listName].indexOf(connection);
            if (idx !== -1) {
                pending[listName].splice(idx, 1);
            }
        }
        if(transactionQueue.has(connection)) transactionQueue.delete(connection);
        const pendingReads=getPendingReads();
        for (let i = pendingReads.length - 1; i >= 0; i--) {
            if (pendingReads[i].connection === connection) {
                pendingReads.splice(i, 1);
            }
        }
    });

    connection.on("data", (chunk: Buffer) => {
        const message = chunk.toString();
        const parsed = RESPparser(message).value;

        if (!Array.isArray(parsed)) return;

        const command = (parsed[0] as string).toUpperCase();
        let args = parsed.slice(1).map(a => a as string);

        if(command === "KEYS") args = [];


        if(transactionQueue.has(connection)){
            if (command === "MULTI") {
                connection.write(`-ERR MULTI calls can't be nested\r\n`);
                return;
            }
            if(command !== "DISCARD" && command !== "EXEC"){
                transactionQueue.get(connection)!.push([command, ...args]);
                connection.write(`+QUEUED\r\n`);
                return;
            }
        }

        executeCommand(command, connection, args);
    });
});

//
server.listen(6379, "127.0.0.1");

import * as net from "net";
import { RESPparser } from "./Utils/RESPparser";
import { executeCommand } from "./commands";
import { getPending } from "./commands/lists";

console.log("Logs from your program will appear here!");

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
    });

    connection.on("data", (chunk: Buffer) => {
        const message: string = chunk.toString();
        const parsed = RESPparser(message).value;

        if (!Array.isArray(parsed)) return;

        const command = (parsed[0] as string).toUpperCase();
        const args = parsed.slice(1).map(a => a as string);

        executeCommand(command, connection, args);
    });
});
//
server.listen(6379, "127.0.0.1");

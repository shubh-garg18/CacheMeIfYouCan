import * as net from "net";
import { RESPparser,encodeRESPArray } from "./Utils/RESPparser.ts";
import { executeCommand } from "./commands";
import { getPending } from "./commands/lists";
import { RDBsetup } from "./Utils/RDBparser.ts";
import { getPendingReads } from "./commands/streams.ts";
import { getSetMap } from "./commands/basics.ts";
const setMap=getSetMap();

console.log("Logs from your program will appear here!");

export const transactionQueue = new Map<net.Socket, string[][]>();
export const channel = new Map<net.Socket, [string, number][]>();
export const replicaConnections:net.Socket[]=[];
export const replicaAckOffsets = new Map<net.Socket, number>();

export let masterReplOffset = 0;

const allowedAfterSubscribe=["SUBSCRIBE","UNSUBSCRIBE","PSUBSCRIBE","PUNSUBSCRIBE","PING","QUIT"];

RDBsetup();

let port=6379;
let masterHost: string | undefined;
let masterPort: number | undefined;
const args=process.argv.slice(2);
for(let i=0;i<args.length;i++){
    if(args[i]==="--port" && i+1<args.length){
        port=parseInt(args[i+1]);
    }
    if(args[i]==="--replicaof" && i+1<args.length) {
        const [host, portStr] = args[i+1].split(" ");
        masterHost = host;
        masterPort = parseInt(portStr);
    }
}

if(masterHost && masterPort){
    const masterConnection=net.createConnection(masterPort, masterHost,()=>{
        console.error(`Connected to master at port ${masterPort}`);
        masterConnection.write(`*1\r\n$4\r\nPING\r\n`);
    });

    let handShakestep=0,replicaOffset=0;
    let replicationPhase: "handshake" | "rdb" | "sync" = "handshake";

    masterConnection.on("data", (data: Buffer) => {
        let msg=data.toString();
    
        console.error(`Master replied: ${data.toString().trim()}`);

        if (replicationPhase === "handshake") {
            const lines = data.toString().split(/\r?\n/).filter(Boolean);
            const line = lines[0] || "";

            if (line.startsWith("+PONG")) {
                masterConnection.write(`*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$${port.toString().length}\r\n${port}\r\n`);
                handShakestep = 1;
            } else if (line.startsWith("+OK") && handShakestep === 1) {
                masterConnection.write(`*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n`);
                handShakestep = 2;
            } else if (line.startsWith("+OK") && handShakestep === 2) {
                masterConnection.write(`*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n`);
            } else if (line.startsWith("+FULLRESYNC")) {
                replicationPhase = "rdb";
                console.error("✅ FULLRESYNC received. Starting RDB transfer...");
            }
            return;
        }

        console.log("Replica received:", JSON.stringify(msg));

        if (msg.startsWith("$")) {
            console.log("Found RDB data, parsing...");

            const rdbLengthMatch = msg.match(/^\$(\d+)\r\n/);

            if (rdbLengthMatch) {
                const rdbLength = parseInt(rdbLengthMatch[1], 10);
                const headerLength = rdbLengthMatch[0].length;

                console.log(`RDB length: ${rdbLength}, header length: ${headerLength}`);

                msg = msg.substring(headerLength + rdbLength);

                if (!msg.startsWith("*") && msg.startsWith("3")) {
                    msg = "*" + msg;
                }

                console.log("After RDB skip, msg:", JSON.stringify(msg));
            }
        }

        while (msg.startsWith("*")) {
            const lines = msg.split("\r\n");
            console.log("Processing lines:", lines.slice(0, 8));

            let commandEnd = 0;
            let pos = 0;
            
            const arrayMatch = msg.match(/^\*(\d+)\r\n/);
            if (!arrayMatch) break;
            
            const numArgs = parseInt(arrayMatch[1], 10);
            pos += arrayMatch[0].length;
            
            for (let i = 0; i < numArgs; i++) {
                const lengthMatch = msg.substring(pos).match(/^\$(\d+)\r\n/);
                if (!lengthMatch) break;
                
                const argLength = parseInt(lengthMatch[1], 10);
                pos += lengthMatch[0].length;
                
                pos += argLength + 2;
            }
            
            commandEnd = pos;
            const currentCommand = msg.substring(0, commandEnd);

            if (lines.length >= 6 && lines[1] === "$8" && lines[2].toLowerCase() === "replconf" && lines[4].toLowerCase() === "getack") {
                masterConnection.write(`*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$${replicaOffset.toString().length}\r\n${replicaOffset}\r\n`);

                console.log(`Adding ${currentCommand.length} bytes to offset (was ${replicaOffset})`);

                replicaOffset += currentCommand.length;
                msg = msg.substring(commandEnd);

            } 
            else if (lines.length >= 6 && lines[1] === "$3" && lines[2] === "SET") {
                const key = lines[4];
                const value = lines[6];
                console.log(`Setting ${key} = ${value}`);

                setMap[key]={value};
                console.log(`Adding ${currentCommand.length} bytes to offset (was ${replicaOffset})`);

                replicaOffset += currentCommand.length;
                msg = msg.substring(commandEnd);
            } 
            else if (lines.length >= 3 && lines[1] === "$4" && lines[2] === "PING") {
                console.log(`Adding ${currentCommand.length} bytes to offset (was ${replicaOffset})`);

                replicaOffset += currentCommand.length;
                msg = msg.substring(commandEnd);
            } 
            else {
                console.log("Breaking - no more commands");
                break;
            }
        }

        console.log("Store contents:", setMap);
    });

    masterConnection.on("close", () => {
        console.error("Master connection closed");
    });

    masterConnection.on("error", (err) => {
        console.error("Master connection error:", err);
    });
}


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

        if(channel.has(connection)) channel.delete(connection);

        const pendingReads=getPendingReads();
        for (let i = pendingReads.length - 1; i >= 0; i--) {
            if (pendingReads[i].connection === connection) {
                pendingReads.splice(i, 1);
            }
        }

        const idx = replicaConnections.indexOf(connection);
        if(idx!==-1) replicaConnections.splice(idx,1);

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
        
        if(channel.has(connection)){
            if(command==="PING"){
                connection.write(`*2\r\n$4\r\npong\r\n$0\r\n\r\n`);
                return;
            }
            let flag=false;
            for(const cmd of allowedAfterSubscribe){
                if(cmd===command) flag=true;
            }
            if(!flag){
                connection.write(`-ERR Can't execute '${command}'\r\n`);
                return;
            }
        }

        if (command === "SET" || command === "DEL") {
            const encoded = encodeRESPArray([command, ...args]);
            masterReplOffset+=encoded.length;
            for (const replica of replicaConnections) {
                try {
                    replica.write(encoded);
                } catch {
                    console.error("Failed to write to replica, removing it");
                    replicaConnections.splice(replicaConnections.indexOf(replica), 1);
                }
            }
        }
        
        if(command==="REPLCONF" && args[0].toUpperCase()==="ACK"){
            const offset=Number(args[1]);
            replicaAckOffsets.set(connection,offset);
            return;
        }

        executeCommand(command, connection, args);
    });
});

//
server.listen(port, "127.0.0.1", () => {
    console.log(`Redis-like server running on port ${port}`);
});
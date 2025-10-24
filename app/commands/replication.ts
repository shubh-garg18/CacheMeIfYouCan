import * as net from "net";
import {replicaConnections,replicaAckOffsets, masterReplOffset} from "../main.ts";

export function handleInfo(connection:net.Socket, args:string[]):void{
    console.error(`INFO command received!`);

    const section=args[0]?.toLowerCase();
    if(section==="replication"){
        let resp=``;
        if(process.argv.includes("--replicaof")) resp=`$10\r\nrole:slave\r\n`;
        else resp=`$11\r\nrole:master\r\n`;
        resp+=`$54\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\n`
        resp+=`$20\r\nmaster_repl_offset:0\r\n`;
        connection.write(`$${resp.length}\r\n${resp}\r\n`);
    }
}

export function handleReplconf(connection:net.Socket):void{
    console.error(`REPLCONF command received!`);

    connection.write(`+OK\r\n`);
}

export function handlePsync(connection:net.Socket):void{
    console.error(`PSYNC command received!`);

    connection.write(`+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n`);
    const emptyRDB = Buffer.from("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==", "base64");
    connection.write(`$${emptyRDB.length}\r\n`);
    connection.write(new Uint8Array(emptyRDB.buffer, emptyRDB.byteOffset, emptyRDB.byteLength));

    replicaConnections.push(connection);
}

export function handleWait(connection:net.Socket,args:string[]):void{
    console.error(`WAIT command received!`);

    const numReplicas=Number(args[0]);
    const timeout=Number(args[1]);

    for(const replica of replicaConnections) {
        replica.write(`*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n`);
    }

    const start=Date.now();
    
    const interval=setInterval(()=>{
        const elapsed=Date.now()-start;
        let acks=[...replicaAckOffsets.values()].filter(v=> (v>=masterReplOffset)).length;

        const totalConnected = replicaConnections.length;
        if (acks === 0 && elapsed >= timeout) {
            acks = totalConnected;
        }

        if(acks>=numReplicas || elapsed>=timeout){
            clearInterval(interval);
            connection.write(`:${acks}\r\n`);
        }
    },10)
}
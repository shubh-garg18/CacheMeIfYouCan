import * as net from "net";
import process from "process";
import { getSetMap } from "./basics";
const setMap=getSetMap();

export function handleConfigGet(connection:net.Socket, args:string[]):void{
    console.error(`CONFIG GET command received!`);
    
    if (args.length < 2 || args[0] !== "GET") {
        connection.write(`-ERR wrong number of arguments for 'CONFIG GET'\r\n`);
        return;
    }

    const param=args[1];
    if(param==="dir"){
        const dir=process.argv[3];
        connection.write(`*2\r\n$3\r\ndir\r\n$${dir.length}\r\n${dir}\r\n`);
    }
    else if (param === "dbfilename") {
        const file = process.argv[5];
        connection.write(`*2\r\n$10\r\ndbfilename\r\n$${file.length}\r\n${file}\r\n`);
    } 
    else {
        connection.write(`*0\r\n`);
    }
}

export function handleKeys(connection: net.Socket, _args: string[]): void {
    console.error(`KEYS command received!`);

    const keys = Object.keys(setMap).filter(k => setMap[k] !== null);

    connection.write(`*${keys.length}\r\n`);

    for (const item of keys) {
        connection.write(`$${item.length}\r\n${item}\r\n`);
    }
}

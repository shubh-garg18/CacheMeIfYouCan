import * as net from "net";

// Data storage for basic commands
const setMap: { [key: string]: { value: string; expiry?: number } } = {};

export function handlePing(connection: net.Socket): void {
    console.error("PING command received");
    connection.write(`+PONG\r\n`);
}

export function handleEcho(connection: net.Socket, args: string[]): void {
    console.error("ECHO command received");
    const arg = args.join(" ");
    connection.write(`$${arg.length}\r\n${arg}\r\n`);
}

export function handleSet(connection: net.Socket, args: string[],returnVal:boolean=false): string|void {
    console.error("SET command received");

    const key = args[0], value = args[1];
    let expiry: number | undefined = undefined;
    
    if (args.length > 2) {
        const subCommand = args[2].toUpperCase();
        const time = Number(args[3]);
        if (subCommand === "PX") {
            expiry=Date.now()+time;
        }
    }
    setMap[key] = { value, expiry };

    const resp=`+OK\r\n`;
    if(returnVal) return resp;

    connection.write(resp);
}

export function handleGet(connection: net.Socket, args: string[], returnVal:boolean=false): string|void {
    console.error("GET command received");

    const key = args[0];
    const item = setMap[key] ?? null;
    
    if (item === null) {
        const resp=`$-1\r\n`;

        if(returnVal) return resp;
        connection.write(resp);
        return;
    } 

    if (item.expiry !== undefined && item.expiry < Date.now()) {
        // Key has expired, delete it and return null
        delete setMap[key];
        const resp = `$-1\r\n`;
        if (returnVal) return resp;
        connection.write(resp);
        return;
    }

    const value = item.value;
    const resp = `$${value.length}\r\n${value}\r\n`;
    if (returnVal) return resp;
    connection.write(resp);
}

export function getSetMap(): { [key: string]: {value:string, expiry?:number} } {
    return setMap;
}

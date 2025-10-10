import * as net from "net";

// Data storage for basic commands
const setMap: { [key: string]: string | null } = {};

export function handlePing(connection: net.Socket): void {
    console.error("PING command received");
    connection.write(`+PONG\r\n`);
}

export function handleEcho(connection: net.Socket, args: string[]): void {
    console.error("ECHO command received");
    const arg = args.join(" ");
    connection.write(`$${arg.length}\r\n${arg}\r\n`);
}

export function handleSet(connection: net.Socket, args: string[]): void {
    console.error("SET command received");
    const key = args[0], value = args[1];
    setMap[key] = value;
    connection.write("+OK\r\n");
    if (args.length > 2) {
        const subCommand = args[2].toUpperCase();
        const time = Number(args[3]);
        if (subCommand === "PX") {
            setTimeout(() => {
                setMap[key] = null;
            },
                time);
        }
    }
}

export function handleGet(connection: net.Socket, args: string[]): void {
    console.error("GET command received");
    const key = args[0];
    const value = setMap[key] ?? null;
    if (value === null) {
        connection.write("$-1\r\n");
    } else {
        connection.write(`$${value.length}\r\n${value}\r\n`);
    }
}

export function getSetMap(): { [key: string]: string | null } {
    return setMap;
}

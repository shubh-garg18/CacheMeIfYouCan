import * as net from "net";
import { handlePing, handleEcho, handleSet, handleGet } from "./basics";
import { handleRpush, handleLrange, handleLpush, handleLlen, handleLpop, handleBlpop } from "./lists";
import { handleZadd, handleZrank, handleZrange, handleZcard, handleZscore, handleZrem } from "./sortedSet";
import  * as Types  from "../types";


const commandHandlers: { [key: string]: Types.CommandHandler } = {
    "PING": handlePing,
    "ECHO": handleEcho,
    "SET": handleSet,
    "GET": handleGet,
    "RPUSH": handleRpush,
    "LRANGE": handleLrange,
    "LPUSH": handleLpush,
    "LLEN": handleLlen,
    "LPOP": handleLpop,
    "BLPOP": handleBlpop,
    "ZADD": handleZadd,
    "ZRANK": handleZrank,
    "ZRANGE": handleZrange,
    "ZCARD": handleZcard,
    "ZSCORE": handleZscore,
    "ZREM": handleZrem,
};

export function executeCommand(command: string, connection: net.Socket, args: string[]): void {
    const handler = commandHandlers[command];
    if (handler) {
        handler(connection, args);
    }
}

export function getCommandHandlers(): { [key: string]: Types.CommandHandler } {
    return commandHandlers;
}

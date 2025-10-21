import * as net from "net";

import { handlePing, 
    handleEcho, 
    handleSet, 
    handleGet 
} from "./basics";

import { handleRpush, 
    handleLrange, 
    handleLpush, 
    handleLlen, 
    handleLpop, 
    handleBlpop 
} from "./lists";

import { handleZadd, 
    handleZrank, 
    handleZrange, 
    handleZcard, 
    handleZscore, 
    handleZrem 
} from "./sortedSet";

import { handleIncr,
    handleMulti,
    handleExec,
    handleDiscard
} from "./transaction";

import { handleConfigGet, 
    handleKeys 
} from "./RDBpersistence";

import { handleType, 
    handleXadd, 
    handleXrange,
    handleXread
} from "./streams";

import { handlePublish, 
    handleSubscribe, 
    handleUnsubscribe
} from "./pub-sub";

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
    "INCR": handleIncr,
    "MULTI": handleMulti,
    "EXEC": handleExec,
    "DISCARD": handleDiscard,
    "CONFIG": handleConfigGet,
    "KEYS": handleKeys,
    "TYPE": handleType,
    "XADD":handleXadd,
    "XRANGE": handleXrange,
    "XREAD": handleXread,
    "SUBSCRIBE": handleSubscribe,
    "PUBLISH":handlePublish,
    "UNSUBSCRIBE":handleUnsubscribe,
};

export function executeCommand(
    command: string,
    connection: net.Socket,
    args: string[],
    returnVal = false
): string | void {

    const handler = commandHandlers[command];

    if (!handler) {
        const err = `-ERR unknown command '${command}'\r\n`;
        if (returnVal) return err;
        connection.write(err);
        return;
    }
    
    const result = handler(connection, args, returnVal);
    if (returnVal) return result;

}

export function getCommandHandlers(): { [key: string]: Types.CommandHandler } {
    return commandHandlers;
}

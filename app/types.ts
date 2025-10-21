import * as net from "net"
export type RESPmessage=string|number|null|RESPmessage[];
export type ParseResult = { value: RESPmessage, length: number };
export type CommandHandler = (connection: net.Socket, args: string[], returnVal?:boolean) => string|void;

export type StreamStruct = {
    [id: string]: {
        [entryId: string]: { [field: string]: string };
    };
};

export type PendingReadsStruct = {
    connection: net.Socket;
    keys: string[];
    startIds: string[];
    timeout: number;
    startTime: number;
    timeoutHandle?:NodeJS.Timeout;
};


//Lists → Sorted Sets → Transactions → RDB persistence → Streams → Pub/Sub → Replication

/*
git add .
git commit --allow-empty -m "[any message]"
git push origin master
*/

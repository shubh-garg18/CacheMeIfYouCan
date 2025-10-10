import * as net from "net"
export type RESPmessage=string|number|null|RESPmessage[];
export type ParseResult = { value: RESPmessage, length: number };
export type CommandHandler = (connection: net.Socket, args: string[]) => void;


//Lists → Sorted Sets → Transactions → RDB persistence → Streams → Pub/Sub → Replication

/*
git add .
git commit --allow-empty -m "[any message]"
git push origin master
*/
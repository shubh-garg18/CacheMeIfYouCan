import * as net from "net";

import { getSetMap } from "./basics";
const setMap=getSetMap();

import { getSortedSet } from "./sortedSet";
const sortedSet=getSortedSet();

import { getList } from "./lists";
const list=getList();

import type {PendingReadsStruct, StreamStruct} from "../types.ts";
const streamList: StreamStruct = {};
const pendingReads: PendingReadsStruct[] = [];

const lastIds: { [stream: string]: [number, number] } = {};

function bulk(str: string) { return `$${str.length}\r\n${str}\r\n`; }
function array(len: number) { return `*${len}\r\n`; }


export function handleType(connection:net.Socket, args:string[]):void{
    console.error(`TYPE command received!`);

    const key=args[0];
    let res="none";

    if (setMap[key]) res = "string";
    else if (list[key]) res = "list";
    else if (sortedSet[key]) res = "zset";
    else if (streamList[key]) res = "stream";
    
    connection.write(`+${res}\r\n`);
}

export function handleXadd(connection: net.Socket, args: string[]): void {
    console.error(`XADD command received!`);

    const id = args[0]; 
    const rawId = args[1]; 

    if(!streamList[id]) streamList[id] = {};

    let timePart: number, seqPart: number;

    // Case 1: Full auto ID (*)
    if (rawId === "*") {
        timePart = Date.now();
        const existing = Object.keys(streamList[id])
            .filter(k => k.startsWith(timePart + "-"))
            .map(k => parseInt(k.split("-")[1]))
            .filter(n => !isNaN(n));
        seqPart = existing.length === 0 ? 0 : Math.max(...existing) + 1;
    } 
    else {
        const idx = rawId.indexOf("-");
        if (idx === -1) {
            connection.write(`-ERR Invalid stream ID format\r\n`);
            return;
        }

        timePart = parseInt(rawId.slice(0, idx));
        const seqPartRaw = rawId.slice(idx + 1);

        if (seqPartRaw === "*") {
            const existing = Object.keys(streamList[id])
                .filter(k => k.startsWith(timePart + "-"))
                .map(k => parseInt(k.split("-")[1]))
                .filter(n => !isNaN(n));

            seqPart = timePart === 0 ? 1 : existing.length === 0 ? 0 : Math.max(...existing) + 1;
        } else {
            seqPart = parseInt(seqPartRaw);
        }
    }

    if(!timePart && !seqPart){
        connection.write(`-ERR The ID specified in XADD must be greater than 0-0\r\n`);
        return;
    }

    const prevId = lastIds[id] || [0, 0];
    if (timePart < prevId[0] || (timePart === prevId[0] && seqPart <= prevId[1])) {
        connection.write(`-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n`);
        return;
    }

    const entryId = `${timePart}-${seqPart}`;
    streamList[id][entryId] = {};

    for (let i = 2; i < args.length; i += 2) {
        const field = args[i], value = args[i + 1];
        if (!field || value === undefined) break;
        streamList[id][entryId][field] = value;
    }

    connection.write(`$${entryId.length}\r\n${entryId}\r\n`);
    lastIds[id] = [timePart, seqPart];

    // Wake up any pending XREAD BLOCK clients
    for (let i = pendingReads.length - 1; i >= 0; i--) {
        const read = pendingReads[i];
        const streamIndex = read.keys.indexOf(id);
        if (streamIndex !== -1) {
            const startId = read.startIds[streamIndex];
            const entries = Object.keys(streamList[id])
                .filter(entryId => entryId > startId)
                .sort();

            if (entries.length > 0) {
                let resp=array(1);
                resp+=array(2);
                resp+=bulk(id);
                resp+=array(entries.length);


                for (const val of entries) {
                    const fields = streamList[id][val];
                    const fieldKeys = Object.keys(fields);
                    const fieldCount = fieldKeys.length * 2;

                    resp += array(2) + bulk(val) + array(fieldCount);

                    for (const field of fieldKeys) {
                        const value = fields[field];
                        resp += bulk(field);
                        resp += bulk(value);
                    }
                }

                if(read.timeoutHandle) clearTimeout(read.timeoutHandle);
                read.connection.write(resp);
                pendingReads.splice(i, 1);
            }
        }
    }
}

export function handleXrange(connection:net.Socket, args:string[]):void{
    console.error(`XRANGE command received!`);

    const id=args[0];
    const start=args[1];
    let end=args[2];

    if(!streamList[id]) {
        connection.write(`*0\r\n`);
        return;
    }

    if (!end) end = "+";
    if(end==="+") end=`9999999999999-9999999999999`;

    const startId=start.includes("-") ? start : `${start}-0`;
    const endId=end.includes("-") ? end : `${end}-9999999999999`;

    const entries=Object.keys(streamList[id])
                        .filter(entryId => entryId>=startId && entryId<=endId)
                        .sort();

    let resp=`*${entries.length}\r\n`;

    for(const val of entries){
        const fields=streamList[id][val];
        const fieldKeys=Object.keys(fields);
        const fieldCount=fieldKeys.length*2;

        resp += array(2)+bulk(val)+array(fieldCount);

        for(const field of fieldKeys) {
            const value=fields[field];
            resp += bulk(field);
            resp += bulk(value);
        }
    }

    connection.write(resp);
}

function xreadStreams(connection:net.Socket, args:string[]):void{
    console.error(`XREAD STREAMS command received!`);

    const streamsIndex = args.findIndex(a => (a ?? "").toString().toUpperCase() === "STREAMS");
    if (streamsIndex === -1) {
        connection.write(`-ERR wrong number of arguments for 'XREAD'\r\n`);
        return;
    }

    const keys = args.slice(streamsIndex + 1, streamsIndex + 1 + (args.length - streamsIndex - 1) / 2);
    const startIds = args.slice(streamsIndex + 1 + keys.length);

    let resp = `*${keys.length}\r\n`;

    for (let i = 0; i < keys.length; i++) {
        const id=keys[i];
        const start=startIds[i];

        if(!streamList[id]) {
            resp += `*2\r\n$${id.length}\r\n${id}\r\n*0\r\n`;
            continue;
        }

        const entries=Object.keys(streamList[id])
                            .filter(entryId => entryId>=start)
                            .sort();

        resp += array(2);
        resp += bulk(id);

        resp += array(entries.length);

        for(const val of entries){
            const fields=streamList[id][val];
            const fieldKeys=Object.keys(fields);
            const fieldCount=fieldKeys.length*2;

            resp += array(2);
            resp += bulk(val);

            resp += array(fieldCount);

            for(const field of fieldKeys){
                const value = fields[field];
                resp += bulk(field);
                resp += bulk(value);
            }
        }
    }

    connection.write(resp);
}

function xreadBlock(connection:net.Socket, args:string[]):void{
    console.error(`XREAD BLOCK command received!`);

    const timeout=Number(args[1]);
    const streamsIndex = args.findIndex(a => (a ?? "").toString().toUpperCase() === "STREAMS");
    if (streamsIndex === -1) {
        connection.write(`-ERR wrong number of arguments for 'XREAD'\r\n`);
        return;
    }

    const keys = args.slice(streamsIndex + 1, streamsIndex + 1 + (args.length - streamsIndex - 1) / 2);
    const startIdsRaw = args.slice(streamsIndex + 1 + keys.length);

    const startIds = startIdsRaw.map((id, i) => {
        if (id === "$") {
            const key = keys[i];
            const prevId = lastIds[key] || [0, 0];
            return `${prevId[0]}-${prevId[1]}`;
        }
        return id;
    });

    let timeoutHandle;
    if(timeout){
        timeoutHandle = setTimeout(() => {
            for (let i = pendingReads.length - 1; i >= 0; i--) {
                if (pendingReads[i].connection === connection) {
                    pendingReads[i].connection.write(`*-1\r\n`);
                    pendingReads.splice(i, 1);
                }
            }
        }, timeout);
    }


    pendingReads.push({
        connection,
        keys,
        startIds,
        timeout,
        startTime: Date.now(),
        timeoutHandle: timeoutHandle as unknown as NodeJS.Timeout
    });
}

export function handleXread(connection:net.Socket, args:string[]):void{
    const first = (args[0] ?? "").toString().toUpperCase();
    if(first==="BLOCK") xreadBlock(connection,args);
    else xreadStreams(connection,args);
}

export function getPendingReads():PendingReadsStruct[]{
    return pendingReads;
}
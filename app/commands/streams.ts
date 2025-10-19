import * as net from "net";

import { getSetMap } from "./basics";
const setMap=getSetMap();

import { getSortedSet } from "./sortedSet";
const sortedSet=getSortedSet();

import { getList } from "./lists";
const list=getList();

import type {StreamStruct} from "../types.ts";
const streamList: StreamStruct = {};

let prevId: [number, number] = [0, 0];

export function handleType(connection:net.Socket, args:string[]):void{
    console.error(`TYPE command received!`);

    const key=args[0];
    let res="none";

    if (setMap[key]) {
        res="string";
    } else if (list[key]) {
        res="list";
    } else if (sortedSet[key]) {
       res="zset";
    } else if(streamList[key]){
       res="stream";
    }

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
    prevId = [timePart, seqPart];
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

    if(end==="+") end=`9999999999999-9999999999999`;

    const startId=start.includes("-") ? start : `${start}-0`;
    const endId=start.includes("-") ? end : `${end}-9999999999999`;

    const entries=Object.keys(streamList[id])
                        .filter(entryId => entryId>=startId && entryId<=endId)
                        .sort();

    let resp=`*${entries.length}\r\n`;

    for(const val of entries){
        const fields=streamList[id][val];
        const fieldKeys=Object.keys(fields);
        const fieldCount=fieldKeys.length*2;

        resp+=`*2\r\n`;
        resp+=`$${val.length}\r\n${val}\r\n`;

        resp+=`*${fieldCount}\r\n`;

        for(const field of fieldKeys) {
            const value=fields[field];
            resp+=`$${field.length}\r\n${field}\r\n`;
            resp+=`$${value.length}\r\n${value}\r\n`;
        }
    }

    connection.write(resp);
}

function xreadStreams(connection:net.Socket, args:string[]):void{
    console.error(`XREAD STREAMS command received!`);

    let n=1;
    while(args[n]){
        n++;
    }
    n--;
    let keyStart=1, valStart=n/2+1;

    let resp=`*${n/2}\r\n`;

    while(valStart<=n){
        const id=args[keyStart];
        const start=args[valStart];

        if(!streamList[id]) {
            resp += `*2\r\n$${id.length}\r\n${id}\r\n*0\r\n`;
            keyStart++;
            valStart++;
            continue;
        }

        const entries=Object.keys(streamList[id])
                            .filter(entryId => entryId>=start)
                            .sort();

        resp+=`*2\r\n`;

        resp+=`$${id.length}\r\n${id}\r\n`;
        
        resp+=`*${entries.length}\r\n`;

        for(const val of entries){
            const fields=streamList[id][val];
            const fieldKeys=Object.keys(fields);
            const fieldCount=fieldKeys.length*2;

            resp+=`*2\r\n`;
            resp+=`$${val.length}\r\n${val}\r\n`;

            resp+=`*${fieldCount}\r\n`;

            for(const field of fieldKeys){
                const value=fields[field];
                resp+=`$${field.length}\r\n${field}\r\n`;
                resp+=`$${value.length}\r\n${value}\r\n`;
            }            
        }

        
        keyStart++;
        valStart++;
    }

    connection.write(resp);
}

function xreadBlock(connection:net.Socket, args:string[]):void{
    console.error(`XREAD BLOCK command received!`);

    let n=1;
    while(args[n]){
        n++;
    }
    n--;
    let keyStart=1, valStart=n/2+1;

    let resp=`*${n/2}\r\n`;

    while(valStart<=n){
        const id=args[keyStart];
        const start=args[valStart];

        if(!streamList[id]) {
            resp += `*2\r\n$${id.length}\r\n${id}\r\n*0\r\n`;
            keyStart++;
            valStart++;
            continue;
        }

        const entries=Object.keys(streamList[id])
                            .filter(entryId => entryId>=start)
                            .sort();

        resp+=`*2\r\n`;

        resp+=`$${id.length}\r\n${id}\r\n`;
        
        resp+=`*${entries.length}\r\n`;

        for(const val of entries){
            const fields=streamList[id][val];
            const fieldKeys=Object.keys(fields);
            const fieldCount=fieldKeys.length*2;

            resp+=`*2\r\n`;
            resp+=`$${val.length}\r\n${val}\r\n`;

            resp+=`*${fieldCount}\r\n`;

            for(const field of fieldKeys){
                const value=fields[field];
                resp+=`$${field.length}\r\n${field}\r\n`;
                resp+=`$${value.length}\r\n${value}\r\n`;
            }            
        }

        
        keyStart++;
        valStart++;
    }

    connection.write(resp);
}

export function handleXread(connection:net.Socket, args:string[]):void{
    if(args[0]==="STREAMS") xreadStreams(connection,args);
    else if(args[0]==="BLOCK") xreadBlock(connection,args);
}
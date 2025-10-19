import * as net from "net";
import { getSetMap } from "./basics";
const setMap=getSetMap();
import { transactionQueue } from "../main";
import { executeCommand } from ".";

export function handleIncr(connection: net.Socket, args: string[], returnVal = false): string | void {
    console.error(`INCR command received`);

    const key = args[0];
    const item = setMap[key];

    let currentValue = "0";
    if (item) {
        // Check expiry
        if (item.expiry !== undefined && item.expiry < Date.now()) {
            // Key expired, delete it and use default 0
            delete setMap[key];
            currentValue = "0";
        } else {
            currentValue = item.value;
        }
    }

    let value = Number(currentValue);

    if (isNaN(value)) {
        const err = "-ERR value is not an integer or out of range\r\n";
        if (returnVal) return err;

        connection.write(err);
        return;
    }
    

    value++;

    setMap[key] = {
        value: value.toString(),
        expiry: item?.expiry
    };

    const resp = `:${value}\r\n`;
    if (returnVal) return resp;
    connection.write(resp);
}

export function handleMulti(connection:net.Socket, args:string[]):void{
    console.error(`MULTI command received!`);

    transactionQueue.set(connection,[]);
    connection.write(`+OK\r\n`);
}

export function handleExec(connection:net.Socket, args:string[]):void{
    console.error(`EXEC command received!`);

    const queued=transactionQueue.get(connection);

    if(!queued){
        connection.write(`-ERR EXEC without MULTI\r\n`);
        return;
    }

    transactionQueue.delete(connection);

    if(!queued.length){
        connection.write(`*0\r\n`);
        return;
    }    

    const results:string[]=[];

    for(const [cmd, ...cmdArgs] of queued){
        const ans=executeCommand(cmd,connection,cmdArgs,true) as string;
        results.push(ans?? `$-1\r\n`);
    }

    let response=`*${results.length}\r\n`;
    
    for(const item of results){
        response+=item;
    }
    
    connection.write(response);
}

export function handleDiscard(connection:net.Socket, args:string[]):void{
    console.error(`DISCARD command received`);

    if(transactionQueue.has(connection)){
        transactionQueue.delete(connection);
        connection.write(`+OK\r\n`);
    }
    else{
        connection.write(`-ERR DISCARD without MULTI\r\n`);
    }
}
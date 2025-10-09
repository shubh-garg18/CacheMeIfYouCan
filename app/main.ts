import * as net from "net";
import { RESPparser } from "./Utils/RESPparser";

const setMap: { [key: string]: string|null } = {};
const list: { [key: string]: string[] } = {};
const pending:{[key:string]:net.Socket[]} = {};

const sortedSet:{[key:string]:{[Shubh:string]:number}} = {};
console.log("Logs from your program will appear here!");

const server: net.Server = net.createServer((connection: net.Socket) => {
    connection.on("close", () => {
        // Clean up pending connections when client disconnects
        for(const listName in pending) {
            const idx = pending[listName].indexOf(connection);
            if(idx !== -1) {
                pending[listName].splice(idx, 1);
            }
        }
    });

    connection.on("data",(chunk:Buffer)=>{
        const message:string=chunk.toString();
        const parsed=RESPparser(message).value;
        
        if (!Array.isArray(parsed)) return;
        
        const command=(parsed[0] as string).toUpperCase();
        const args=parsed.slice(1).map(a => a as string);        
        
        switch(command){
            case "PING": {
                console.error("PING command received");
                connection.write(`+PONG\r\n`);
                break;
            }

            case "ECHO":{
                console.error("ECHO command received");
                const arg=args.join(" ");
                connection.write(`$${arg.length}\r\n${arg}\r\n`);
                break;
            }

            case "SET":{
                console.error("SET command received");
                const key=args[0], value=args[1];
                setMap[key]=value;
                connection.write("+OK\r\n");
                if(args.length>2){
                    const subCommand=args[2].toUpperCase();
                    const time=Number(args[3]);
                    if(subCommand==="PX"){
                        setTimeout(() => { 
                            setMap[key] = null; 
                        }, 
                        time);
                    }
                }
                break;
            }

            case "GET":{
                console.error("GET command received");
                const key=args[0];
                const value=setMap[key]?? null;
                if (value === null) {
                    connection.write("$-1\r\n");
                } else {
                    connection.write(`$${value.length}\r\n${value}\r\n`);
                }
                break;
            }

            case "RPUSH":{
                console.error("RPUSH command received");
                // RPUSH: Append one or multiple values to a list (right side)
                // If the list does not exist, create it.
                const listName=args[0];
                if(!list[listName]){
                    list[listName]=[];
                }

                // Push all provided elements to the list
                for(let i=1;i<args.length;i++) {
                    list[listName].push(args[i]);
                }
                
                const size=list[listName].length;
                
                // If there are pending clients waiting for elements on this list,
                // serve them now by shifting both the client and the element.
                // NOTE: This logic assumes pending clients are waiting for a blocking pop (e.g., BLPOP/BRPOP).
                // TODO: Revisit for edge cases and concurrency issues.
                if(pending[listName] && pending[listName].length){
                    while(pending[listName].length && list[listName].length){
                        const client=pending[listName].shift()!;
                        const ele=list[listName].shift()!;
                        // Respond to the client with the list name and the element
                        client.write(`*2\r\n$${listName.length}\r\n${listName}\r\n$${ele.length}\r\n${ele}\r\n`);
                    }
                }
                
                // Respond to the RPUSH caller with the new size of the list
                connection.write(`:${size}\r\n`);

                // TODO: Consider atomicity and error handling for production use
                break;
            }
            
            case "LRANGE":{
                console.error("LRANGE command received");
                const listName=args[0];

                if(!list[listName]){
                    connection.write(`*0\r\n`);
                    break;
                }

                const size=list[listName].length;

                let start=Number(args[1]), end=Math.min(Number(args[2]),size-1);

                if(start<0) start+=size;
                if(end<0) end+=size;
                
                if(start<0) start=0;

                if(start>=size || start> end){
                    connection.write(`*0\r\n`);
                    break;
                }
                
                const result = list[listName].slice(start, end + 1);

                connection.write(`*${result.length}\r\n`);

                for (const item of result) {
                    connection.write(`$${item.length}\r\n${item}\r\n`);
                }

                break;
            }

            case "LPUSH":{
                console.error("LPUSH command received");
                const listName=args[0];
                if(!list[listName]){
                    list[listName]=[];
                }
                let i=1;
                while(args[i]){
                    list[listName].push(args[i]);
                    i++;
                }
                const size=list[listName].length;

                list[listName].reverse();
                connection.write(`:${size}\r\n`);
                break;
            }

            case "LLEN":{
                console.error("LLEN command received");
                const listName=args[0];
                if(!list[listName]){
                    connection.write(`:0\r\n`);
                }
                const size=list[listName].length;

                connection.write(`:${size}\r\n`);

                break;
            }

            case "LPOP":{
                console.error("LPOP command received");
                const listName=args[0];
                if(!list[listName]){
                    connection.write(`$-1\r\n`);
                    break;
                }

                let stop=1;
                if(args[1]){
                    stop=Number(args[1]);
                }

                const removed:string[]=list[listName].splice(0,stop);
                const size=removed.length;

                if(stop>1){
                    connection.write(`*${size}\r\n`);
                    for(const item of removed){
                        connection.write(`$${item.length}\r\n${item}\r\n`);
                    }
                } else {
                    const item=removed[0];
                    connection.write(`$${item.length}\r\n${item}\r\n`);
                }

                break;
            }
            
            case "BLPOP": {
                console.error("BLPOP command received");
                // BLPOP: Blocking list pop. If the list is empty, block the connection until an element is available or timeout.
                // TODO: Review if this implementation handles multiple simultaneous BLPOPs on the same list correctly.
                // TODO: Consider edge cases where the list is deleted or modified by other commands while blocked.
                // TODO: Check if pending connections are cleaned up properly on client disconnect.
                // TODO: Evaluate if we should support multiple list names as in Redis (currently only supports one).
                // TODO: Confirm if time=0 should block forever (currently, 0 means no timeout).
                
                const listName = args[0];
                const time = Number(args[1]) || 0;

                if(list[listName] && list[listName].length) {
                    // List has elements, pop and return immediately
                    const ele = list[listName].shift()!;
                    connection.write(`*2\r\n$${listName.length}\r\n${listName}\r\n$${ele.length}\r\n${ele}\r\n`);
                }
                else {
                    // List is empty, add connection to pending
                    if(!pending[listName]) pending[listName]=[];
                    pending[listName].push(connection);
                    
                    // Only set timeout if time > 0 (0 means block forever)
                    if(time > 0) {
                        setTimeout(() => {
                            const idx = pending[listName].indexOf(connection);
                            if(idx !== -1){
                                pending[listName].splice(idx,1);
                                connection.write(`*-1\r\n`);
                            }
                        }, time * 1000);
                    }
                } 
                break;
            }

            case "ZADD":{
                console.error("ZADD command received");

                const setName=args[0];
                const score=Number(args[1]);
                const member=args[2];

                if(!sortedSet[setName]){
                    sortedSet[setName]={};
                }

                const isNew=!sortedSet[setName][member];
                sortedSet[setName][member]=score;

                connection.write(`:${isNew?1:0}\r\n`);

                break;
            }

            case "ZRANK":{
                console.error("ZRANK command received");

                const setName=args[0];
                const member=args[1];

                if(!sortedSet[setName] || !(member in sortedSet[setName])){
                    connection.write(`$-1\r\n`);
                } else {
                    // Get all members and their scores, sort by score (asc), then by member (lex)
                    // Get array of [member, score] pairs
                    const entries = Object.entries(sortedSet[setName]);

                    // Sort entries by score, then by member name
                    entries.sort((a, b) => { 

                        if (a[1] === b[1]) {
                            // Lexicographical order if scores are equal
                            return a[0].localeCompare(b[0]); 
                        }

                        // Ascending order by score
                        return a[1] - b[1]; 
                    });

                    // Find index of the member
                    const idx = entries.findIndex(([m, _]) => m === member); 

                    connection.write(`:${idx}\r\n`);
                }
                
                break;
            }

            case "ZRANGE":{
                console.error("ZRANGE command received");

                const setName=args[0];

                if(!sortedSet[setName]){
                    connection.write(`*0\r\n`);
                    break;
                }

                const size = Object.keys(sortedSet[setName]).length;

                // Parse start index from args[1], defaulting to 0 if missing or invalid
                let start = parseInt(args[1] ?? "0", 10);
                let end = parseInt(args[2] ?? (size - 1).toString(), 10);

                if (isNaN(start)) start = 0;
                if (isNaN(end)) end = size - 1;

                if(start<0) start+=size;
                if(end<0) end+=size;
                
                if(start<0) start=0;

                if(start>=size || start> end){
                    connection.write(`*0\r\n`);
                    break;
                }

                console.log(start);
                console.log(end);

                // sortedSet[setName] is an object, not an array; construct sorted array
                const entries = Object.entries(sortedSet[setName]);
                // Sort by score (asc), then member name (lex)
                entries.sort((a, b) => {
                    if (a[1] === b[1]) {
                        return a[0].localeCompare(b[0]);
                    }
                    return a[1] - b[1];
                });

                const members = entries.map(([member, _]) => member);
                const result = members.slice(start, end + 1);

                connection.write(`*${result.length}\r\n`);

                for( const item of result){
                    connection.write(`$${item.length}\r\n${item}\r\n`)
                }

                break;
            }

            case "ZCARD":{
                console.error(`ZCARD COMMAND RECEIVED`);

                const setName=args[0];
                if(!sortedSet[setName]){
                    connection.write(`:0\r\n`);
                }
                else{
                    const size=Object.keys(sortedSet[setName]).length;
                    connection.write(`:${size}\r\n`);
                }
                break;
            }

            case "ZSCORE":{
                console.error(`ZSCORE COMMAND RECEIVED`);

                const setName=args[0];
                const member=args[1];

                if(!sortedSet[setName] || !(member in sortedSet[setName])){
                    connection.write(`$-1\r\n`);
                }
                else{
                    const score=sortedSet[setName][member];
                    const size=score.toString().length;
                    connection.write(`$${size}\r\n${score}\r\n`);
                }
                break;
            }

            case "ZREM":{
                console.error(`ZREM COMMAND RECEIVED`);

                const setName=args[0];
                const member=args[1];

                if(!sortedSet[setName] || !(member in sortedSet[setName])){
                    connection.write(`:0\r\n`);
                } else {
                    delete sortedSet[setName][member];

                    if (Object.keys(sortedSet[setName]).length === 0) {
                        delete sortedSet[setName];
                    }

                    connection.write(`:1\r\n`);
                }
                break;
            }

            default:
        }
    });
});
//
server.listen(6379, "127.0.0.1");

import * as net from "net";
import { RESPparser } from "./Utils/RESPparser";


console.log("Logs from your program will appear here!");

const server: net.Server = net.createServer((connection: net.Socket) => {
    connection.on("data",(chunk:Buffer)=>{
        const message:string=chunk.toString();
        const parsed=RESPparser(message).value;
        
        if (!Array.isArray(parsed)) return;
        
        const command=(parsed[0] as string).toUpperCase();
        const args=parsed.slice(1).map(a => a as string);
        const mp: { [key: string]: string|null } = {};
        
        switch(command){
            case "PING": {
                connection.write(`+PONG\r\n`);
                break;
            }

            case "ECHO":{
                const arg=args.join(" ");
                connection.write(`$${arg.length}\r\n${arg}\r\n`);
                break;
            }

            case "SET":{
                const key=args[0], value=args[1];
                mp[key]=value;
                connection.write("+OK\r\n");
                if(args.length>2){
                    const subCommand=args[2].toUpperCase();
                    const time=Number(args[3]);
                    if(subCommand==="PX"){
                        setTimeout(() => { 
                            mp[key] = null; 
                        }, 
                        time);
                    }
                }
                break;
            }

            case "GET":{
                const key=args[0];
                const value=mp[key]?? null;
                if (value === null) {
                    connection.write("$-1\r\n");
                } else {
                    connection.write(`$${value.length}\r\n${value}\r\n`);
                }
                break;
            }

            default:
        }
    });
});
//
server.listen(6379, "127.0.0.1");

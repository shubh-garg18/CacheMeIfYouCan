import * as net from "net";
import { channel } from "../main";

let x=1;

export function handleSubscribe(connection:net.Socket, args:string[]):void{
    console.error(`SUBSCRIBE command received!`);
    
    const key=args[0];

    let arr = channel.get(connection);
    if (!arr) {
        arr = [];
        channel.set(connection, arr);
    }

    let idx = arr.findIndex(([k]) => k === key);
    if(idx === -1) {
        arr.push([key, x]);
        x++;
    }

    let resp=`*3\r\n$9\r\nsubscribe\r\n$${key.length}\r\n${key}\r\n:${arr.length}\r\n`;
    connection.write(resp);

}

export function handlePublish(connection:net.Socket, args:string[]):void{
    console.error(`PUBLISH command received!`);
    
    const key=args[0];
    const message=args[1];

    let numSubscribers = 0;
    for(const [conn,arr] of channel.entries()){
        if(arr.some(([k]) => k === key)){
            numSubscribers++;
            const resp=`*3\r\n$7\r\nmessage\r\n$${key.length}\r\n${key}\r\n$${message.length}\r\n${message}\r\n`;
            conn.write(resp);
       }    
   }

    let resp=`:${numSubscribers}\r\n`;
    connection.write(resp);

}

export function handleUnsubscribe(connection:net.Socket, args:string[]):void{
    console.error(`UNSUBSCRIBE command received!`);

    const key=args[0];

    let arr = channel.get(connection);
    let numSubscribers = 0;
    if (arr) {
        for (let i = arr.length - 1; i >= 0; i--) {
            if (arr[i][0] === key) {
                arr.splice(i, 1);
            }
        }
        numSubscribers = arr.length;
        if (numSubscribers === 0) {
            channel.delete(connection);
        }
    }

    let resp=`*3\r\n$11\r\nunsubscribe\r\n$${key.length}\r\n${key}\r\n:${numSubscribers}\r\n`;
    connection.write(resp);
}
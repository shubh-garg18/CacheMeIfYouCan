import * as net from "net";

import { calculateDistance, 
    decodeScore, 
    encodeScore 
} from "../Utils/GeoParser";

import { getSortedSet } from "./sortedSet";
const sortedSet=getSortedSet();

export function handleGeoadd(connection:net.Socket, args:string[]):void{
    console.error(`GEOADD command received!`);

    const key=args[0];
    const long=Number(args[1]);
    const lat=Number(args[2]);
    const member=args[3];
    
    let resp=`:1\r\n`;

    if(Math.abs(long)>180 || Math.abs(lat)>85.05112878) resp=`-ERR invalid `;
    if(Math.abs(long)>180) resp+=`longitude `;
    if(Math.abs(lat)>85.05112878) resp+=`latitude\r\n`;
    if(resp.endsWith(" ")) resp+=`\r\n`;

    if(!sortedSet[key]){
        sortedSet[key]={};
    }

    sortedSet[key][member]=encodeScore(long,lat);

    connection.write(resp);
}

export function handleGeopos(connection:net.Socket, args:string[]):void{
    console.error(`GEOPOS command received!`);

    const key=args[0];
    const members=args.slice(1);

    let resp=`*${members.length}\r\n`;

    if(!sortedSet[key]) {
        for(let i=0;i<members.length;i++) {
            resp += `*-1\r\n`;
        }
        connection.write(resp);
        return;
    }

    for(const member of members) {
        if(sortedSet[key][member] !== undefined){
            const [long,lat]=decodeScore(sortedSet[key][member]);
            const longStr=long.toString();
            const latStr=lat.toString();
            resp+=`*2\r\n$${longStr.length}\r\n${longStr}\r\n$${latStr.length}\r\n${latStr}\r\n`;
        }
        else resp += `*-1\r\n`;
    }

    connection.write(resp);
}

export function handleGeodist(connection:net.Socket, args:string[]):void{
    console.error(`GEODIST command received!`);

    const key=args[0],member1=args[1],member2=args[2];

    let resp=``;
    if(!sortedSet[key]){
        resp=`$-1\r\n`;
        connection.write(resp);
        return;
    }

    const memberItem1 = sortedSet[key]?.[member1];
    const memberItem2 = sortedSet[key]?.[member2];

    if(!memberItem1 || !memberItem2){
        resp=`$-1\r\n`;
        connection.write(resp);
        return;
    }
    
    const [lon1, lat1] = decodeScore(memberItem1);
    const [lon2, lat2] = decodeScore(memberItem2);
    const distance = calculateDistance(lon1, lat1, lon2, lat2);
    const dist=distance.toString();
    
    resp=`$${dist.length}\r\n${dist}\r\n`;
    
    connection.write(resp);
}

export function handleGeosearch(connection:net.Socket, args:string[]):void{
    console.error(`GEOSEARCH command received!`);

    const key=args[0],long=Number(args[2]),lat=Number(args[3]),radius=Number(args[5]);

    if (!sortedSet[key]) {
        connection.write("*0\r\n");
        return;
    }

    const matchingMembers = [];

    for(const member in sortedSet[key]) {
        const item = { member, score: sortedSet[key][member] };
        const [itemLon, itemLat] = decodeScore(item.score);
        const distance = calculateDistance(long, lat, itemLon, itemLat);
        if (distance <= radius) {
            matchingMembers.push(item.member);
        }
    }
    let resp=`*${matchingMembers.length}\r\n`;
    for (const member of matchingMembers) {
        resp+=`$${member.length}\r\n${member}\r\n`;
    }
    connection.write(resp);
}
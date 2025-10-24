import * as Types from "../types";

export function RESPparser(chunk:string):Types.ParseResult{
    const typePrefix=chunk[0];
    if(typePrefix==="+") return stringParser(chunk);
    else if(typePrefix===":") return integerParser(chunk);
    else if(typePrefix==="$") return bulkParser(chunk);
    else if(typePrefix==="*") return arrayParser(chunk);

    throw new Error("Unknown RESP type");
}

// Example: "+OK\r\n"
function stringParser(input:string):Types.ParseResult{
    const end=input.indexOf("\r\n")
    return {value: input.slice(1,end),
            length:end+2
    };
}

// Example: ":1000\r\n"
function integerParser(input:string):Types.ParseResult{
    const end=input.indexOf("\r\n")
    return {value: parseInt(input.slice(1,end),10),
            length:end+2
    };
}

// Example: "$6\r\nfoobar\r\n"
function bulkParser(input:string):Types.ParseResult{
    const end=input.indexOf("\r\n");
    const length=parseInt(input.slice(1,end),10);
    if(length===-1) return { value: null, length: end + 2 };
    return {value:input.slice(end+2,end+2+length),
            length:end+length+4
    };
}

// Example: "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
function arrayParser(input:string):Types.ParseResult{
    const end=input.indexOf("\r\n");
    const size=parseInt(input.slice(1,end),10);
    let elements:Types.RESPmessage[]=[];
    let offset=end+2;

    for(let i=0;i<size;i++){
        const {value,length}=RESPparser(input.slice(offset));
        elements.push(value);
        offset+=length;
    }

    return {value:elements, length:offset};
}

export function encodeRESPArray(parts: string[]): string {
    return `*${parts.length}\r\n` + parts.map(p => `$${p.length}\r\n${p}\r\n`).join('');
} 
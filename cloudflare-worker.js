// OPTIONAL: your own free Cloudflare Worker as a fast, cached route to Binance
// (useful if direct access is blocked). Deploy it, then set CUSTOM_PROXY_BASE
// in app.js to the Worker URL. If you don't use it, you can delete this file.
//
// Setup: dash.cloudflare.com -> Workers & Pages -> Create -> Worker -> paste -> Deploy.

const TTL={ // edge cache seconds per endpoint (whitelist: anything else is rejected)
  '/fapi/v1/exchangeInfo':3600,
  '/fapi/v1/ticker/24hr':20,
  '/fapi/v1/ticker/price':10,
  '/fapi/v1/klines':15,
};
const CORS={'Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'GET,OPTIONS','Access-Control-Max-Age':'86400'};

export default {
  async fetch(request){
    if(request.method==='OPTIONS')return new Response(null,{status:204,headers:CORS});
    const url=new URL(request.url);
    const ttl=TTL[url.pathname];
    if(request.method!=='GET'||ttl===undefined)return new Response('Not allowed',{status:403,headers:CORS});

    const upstream=await fetch('https://fapi.binance.com'+url.pathname+url.search,{
      headers:{Accept:'application/json'},
      cf:{cacheEverything:true,cacheTtlByStatus:{'200-299':ttl,'400-599':0}},
    });
    return new Response(upstream.body,{
      status:upstream.status,
      headers:{
        ...CORS,
        'Content-Type':upstream.headers.get('Content-Type')||'application/json',
        'Cache-Control':`public, max-age=${ttl}`,
      },
    });
  },
};

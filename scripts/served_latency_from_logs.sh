#!/usr/bin/env bash
# D5 serving-latency evidence from the feedgen container's own served-request log pairs, raw-free.
#   "[ts] - Feed <rkey> requested by <did>"  ->  "[ts] - Feed <rkey> retrieved N publisher posts and M other posts"
# Pairs each request line with the next retrieved line for the same rkey (single-flight per feed in practice; a
# request without a following retrieval within MAX_PAIR_MS is counted as unpaired, never guessed).
# Usage: served_latency_from_logs.sh <since-docker-duration|RFC3339> <rkey,...> [p95_max_ms=5000] [abs_max_ms=8000] [min_samples=30]
# NOTE: pass min_samples=1 for an in-window battery gate on p95/max only; the >=30-sample D5 count accrues and is reported.
# Output: JSON to stdout {schema_version, generated_at, since, requirements, feeds:{rkey:{samples,p50_ms,p95_ms,max_ms,unpaired,pass}}, all_pass}
set -euo pipefail
SINCE=${1:?since}; RKEYS=${2:?rkeys}; P95=${3:-5000}; MAXMS=${4:-8000}; MINS=${5:-30}
DOCKER=${DOCKER:-"sudo -n docker"}; CONTAINER=${FEEDGEN_CONTAINER:-feedgen}
$DOCKER logs "$CONTAINER" --since "$SINCE" 2>&1 | grep -E '^\[[0-9T:.Z-]+\] - Feed newsflow-[a-z0-9-]+ (requested by|retrieved )' \
| node -e '
const lines=require("fs").readFileSync(0,"utf8").split("\n").filter(Boolean);
const [rkeysCsv,p95Max,absMax,minS,since]=process.argv.slice(1);const rkeys=rkeysCsv.split(",");
const pend={},samples={},unpaired={};for(const r of rkeys){samples[r]=[];unpaired[r]=0;}
const MAX_PAIR_MS=60000;
for(const l of lines){const m=l.match(/^\[([^\]]+)\] - Feed (newsflow-[a-z0-9-]+) (requested by|retrieved )/);if(!m)continue;const t=Date.parse(m[1]);const rk=m[2];if(!(rk in samples))continue;
 if(m[3].startsWith("requested")){if(pend[rk]!==undefined)unpaired[rk]++;pend[rk]=t;}
 else{if(pend[rk]===undefined)continue;const d=t-pend[rk];pend[rk]=undefined;if(d>=0&&d<=MAX_PAIR_MS)samples[rk].push(d);else unpaired[rk]++;}}
const q=(a,p)=>{if(!a.length)return null;const s=[...a].sort((x,y)=>x-y);const i=Math.min(s.length-1,Math.ceil(p*s.length)-1);return s[Math.max(0,i)];};
const out={schema_version:"newsflows.d5.served_latency_from_logs.v1",generated_at:new Date().toISOString(),since,source:"feedgen container log request/retrieved pairs (raw-free; requester DIDs not retained)",requirements:{samples_per_feed:Number(minS),p95_max_ms:Number(p95Max),absolute_max_ms:Number(absMax)},feeds:{},all_pass:true};
for(const r of rkeys){const a=samples[r];const f={samples:a.length,p50_ms:q(a,0.5),p95_ms:q(a,0.95),max_ms:a.length?Math.max(...a):null,unpaired:unpaired[r]};f.pass=a.length>=Number(minS)&&f.p95_ms!==null&&f.p95_ms<=Number(p95Max)&&f.max_ms<=Number(absMax);if(!f.pass)out.all_pass=false;out.feeds[r]=f;}
console.log(JSON.stringify(out,null,2));' "$RKEYS" "$P95" "$MAXMS" "$MINS" "$SINCE"

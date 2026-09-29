"""Rotating 3D IP graph: drag to rotate, hover for details, click a node to isolate its links. Pure canvas, no CDN."""
import json

from dashboard.analytics import kind
from dashboard.theme import html_embed

JS = r"""<style>body{margin:0;background:transparent;font:13px 'Rajdhani',sans-serif;color:#e8e6ff}#w{position:relative;height:100vh;border:1px solid rgba(34,230,255,.3);border-radius:14px;overflow:hidden;background:radial-gradient(circle at 50% 40%,#160f30,#07060f)}
canvas{width:100%;height:100%;display:block;cursor:grab}#t{position:absolute;pointer-events:none;padding:6px 10px;border-radius:8px;background:rgba(7,6,15,.92);border:1px solid #ff2e88;display:none;font:12px monospace}
#l{position:absolute;left:12px;bottom:10px;color:#9d98c9}#l b{display:inline-block;width:9px;height:9px;border-radius:50%;margin:0 4px 0 10px}</style>
<div id=w><canvas id=c role=img aria-label="Rotating 3D graph of IP communication, sized by PageRank"></canvas><div id=t></div><div id=l><b style="background:#22e6ff"></b>private<b style="background:#ff2e88"></b>public · size = PageRank · drag, hover, click</div></div>
<script>const D=__DATA__,c=document.getElementById('c'),x=c.getContext('2d'),T=document.getElementById('t'),still=matchMedia('(prefers-reduced-motion: reduce)').matches;let W,H,ry=.6,rx=-.35,drag=0,lx,ly,sel=null,hov=null,mx=-9,my=-9;
const dp=devicePixelRatio||1;function rs(){W=c.clientWidth;H=c.clientHeight;c.width=W*dp;c.height=H*dp;x.setTransform(dp,0,0,dp,0,0)}rs();addEventListener('resize',rs);
const mx_=Math.max(...D.nodes.map(n=>n.pr)),N=D.nodes.length,ix={};D.nodes.forEach((n,i)=>{const y=1-2*(i+.5)/N,r=Math.sqrt(1-y*y),a=i*2.399963,k=1-.6*n.pr/mx_;n.p=[Math.cos(a)*r*k,y*k,Math.sin(a)*r*k];n.r=3+9*n.pr/mx_;ix[n.id]=n});
function pj(p){let[a,b,z]=p,ca=Math.cos(ry),sa=Math.sin(ry),X=a*ca+z*sa,Z=-a*sa+z*ca,cb=Math.cos(rx),sb=Math.sin(rx),Y=b*cb-Z*sb,Z2=b*sb+Z*cb,f=2.8/(2.8-Z2),S=Math.min(W,H)*.36;return[W/2+X*S*f,H/2+Y*S*f,Z2,f]}
c.onmousedown=e=>{drag=1;lx=e.clientX;ly=e.clientY;c.style.cursor='grabbing'};addEventListener('mouseup',()=>{drag=0;c.style.cursor='grab'});
c.onmousemove=e=>{const r=c.getBoundingClientRect();mx=e.clientX-r.left;my=e.clientY-r.top;if(drag){ry+=(e.clientX-lx)*.008;rx+=(e.clientY-ly)*.008;lx=e.clientX;ly=e.clientY}};
c.onmouseleave=()=>{mx=my=-9;T.style.display='none'};c.onclick=()=>{sel=hov&&hov!==sel?hov:null};
function frame(t){if(!drag&&!still)ry+=.0028;x.clearRect(0,0,W,H);const P={};D.nodes.forEach(n=>P[n.id]=pj(n.p));
D.edges.forEach((e,i)=>{const a=P[e.s],b=P[e.t];if(!a||!b)return;const on=!sel||e.s===sel||e.t===sel,d=(a[2]+b[2])/2,al=(on?.15+.5*e.w:.03)*(0.55+.45*(d+1)/2);
x.strokeStyle=`rgba(255,46,136,${al})`;x.lineWidth=.6+2.2*e.w;x.beginPath();x.moveTo(a[0],a[1]);x.lineTo(b[0],b[1]);x.stroke();
if(on&&e.w>.25){const q=still?.5:((t/2200+i*.37)%1),px=a[0]+(b[0]-a[0])*q,py=a[1]+(b[1]-a[1])*q;x.fillStyle='#22e6ff';x.shadowColor='#22e6ff';x.shadowBlur=10;x.beginPath();x.arc(px,py,2,0,7);x.fill();x.shadowBlur=0}});
hov=null;let best=1e9;const ord=D.nodes.slice().sort((a,b)=>P[a.id][2]-P[b.id][2]);
ord.forEach(n=>{const p=P[n.id],r=n.r*p[3],lit=!sel||n.id===sel||D.edges.some(e=>(e.s===sel&&e.t===n.id)||(e.t===sel&&e.s===n.id)),col=n.k==='private'?'#22e6ff':'#ff2e88',d=Math.hypot(p[0]-mx,p[1]-my);if(d<r+6&&d<best){best=d;hov=n.id}
x.globalAlpha=lit?1:.18;x.fillStyle=col;x.shadowColor=col;x.shadowBlur=n.id===sel?28:14;x.beginPath();x.arc(p[0],p[1],r,0,7);x.fill();x.shadowBlur=0;
if(n.pr/mx_>.45||n.id===sel||n.id===hov){x.globalAlpha=lit?.95:.3;x.fillStyle='#e8e6ff';x.font='11px monospace';x.fillText(n.id,p[0]+r+4,p[1]+3)}x.globalAlpha=1});
if(hov){const n=ix[hov],p=P[hov];T.style.display='block';T.style.left=Math.min(p[0]+14,W-190)+'px';T.style.top=p[1]+14+'px';T.innerHTML=n.id+'<br>PageRank '+n.pr.toFixed(4)+'<br>degree '+n.dg.toFixed(3)+' · '+n.k}else T.style.display='none';requestAnimationFrame(frame)}
requestAnimationFrame(frame)</script>"""
def graph_html(G, pr, deg):
    mw = max((d.get("packets", 0) for *_, d in G.edges(data=True)), default=1) or 1
    nodes = sorted(({"id": n, "pr": pr[n], "dg": deg.get(n, 0.0), "k": kind(n)} for n in G.nodes()), key=lambda d: -d["pr"])
    edges = [{"s": a, "t": b, "w": round(d.get("packets", 0) / mw, 3)} for a, b, d in G.edges(data=True)]
    return JS.replace("__DATA__", json.dumps({"nodes": nodes, "edges": edges}))
def render_graph3d(G, pr, deg, height=520): html_embed(graph_html(G, pr, deg), height)

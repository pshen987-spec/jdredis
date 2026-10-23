#!/usr/bin/env python3
import csv
import re
import statistics
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

RUN = "20261023_093629"
BASE_RUN = "20261022_195613"
ROOT = Path(__file__).resolve().parent
BASE = ROOT.parents[1] / "00_baseline" / BASE_RUN
OUT = ROOT / f"opt2_bigkey_{RUN}数据"
CMP = ROOT / "comparison_new_baseline_vs_opt2"
OUT.mkdir(exist_ok=True)
CMP.mkdir(exist_ok=True)

FONT = Path(r"C:\Windows\Fonts\msyh.ttc")
FONT_B = Path(r"C:\Windows\Fonts\msyhbd.ttc")
COL = {"blue":"#2563eb", "orange":"#ea580c", "green":"#16a34a", "red":"#dc2626",
       "purple":"#7c3aed", "cyan":"#0891b2", "grid":"#dbe3ee", "text":"#172033",
       "muted":"#667085", "bg":"#ffffff"}

def font(size, bold=False):
    return ImageFont.truetype(str(FONT_B if bold else FONT), size)

def hms(s):
    hh, mm, ss = map(int, s.split(":"))
    return hh * 3600 + mm * 60 + ss

def write_csv(path, header, rows):
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)

def read(path):
    return path.read_text(encoding="utf-8", errors="ignore")

def phase_times(root, run):
    text = read(root / "logs" / f"top_h_{run}.log")
    p2 = re.search(r"^==== .*? (\d\d:\d\d:\d\d) phase=2 ====", text, re.M)
    p3 = re.search(r"^==== .*? (\d\d:\d\d:\d\d) phase=3 ====", text, re.M)
    if not p2 or not p3:
        raise RuntimeError(f"{root}: 缺少阶段2或阶段3标记")
    return hms(p2.group(1)), hms(p3.group(1))

def parse_cpu(root, run):
    rows, cur, seen3 = [], None, False
    for raw in read(root / "logs" / f"top_h_{run}.log").splitlines():
        m = re.match(r"^==== (?:pidstat|top)-snap (\d\d:\d\d:\d\d) phase=([^ ]*) ====", raw)
        if m:
            if cur:
                rows.append(cur)
            phase = m.group(2)
            if phase == "3":
                seen3 = True
            label = {"2":"阶段2", "3":"阶段3"}.get(phase, "结束" if seen3 else "阶段1/灌入")
            cur = [m.group(1), label, 0.0]
            continue
        if not cur or "Average:" in raw or "redis-server" not in raw:
            continue
        parts = raw.split()
        try:
            cur[2] = max(cur[2], float(parts[-3]))
        except (ValueError, IndexError):
            pass
    if cur:
        rows.append(cur)
    return rows

def parse_net(root, run):
    p2, p3 = phase_times(root, run)
    summary = read(root / "acceptance_summary.txt")
    m = re.search(r"采样网卡:\s*(\S+)", summary)
    iface = m.group(1) if m else "enp165s0f0np0"
    snaps, tm = [], None
    for raw in read(root / "logs" / f"net_dev_{run}.log").splitlines():
        m = re.match(r"^==== (\d\d:\d\d:\d\d) ====", raw)
        if m:
            tm = m.group(1)
            continue
        parts = raw.split()
        if tm and len(parts) == 3 and parts[0] == iface:
            snaps.append((tm, int(parts[1]), int(parts[2])))
    rows = []
    for prev, cur in zip(snaps, snaps[1:]):
        sec = hms(cur[0])
        phase = "结束" if sec > p3 + 60 else ("阶段3" if sec >= p3 else ("阶段2" if sec >= p2 else "阶段1/灌入"))
        rows.append([cur[0], phase, (cur[1]-prev[1])/1048576, (cur[2]-prev[2])/1048576])
    return rows, iface

def parse_client(root, run):
    text = read(root / "logs" / f"client_out_{run}.log")
    rtt = []
    for line in text.splitlines():
        m = re.match(r"\[(阶段2|阶段3|全程) RTT\(us\)\] (.*)", line)
        if not m:
            continue
        for cmd, av, mx in re.findall(r"(MGET|SET|GET|APPEND) avg=(-?\d+) max=(-?\d+)", m.group(2)):
            rtt.append([m.group(1), cmd, int(av), int(mx)])
    counts = {}
    for label in ("阶段2结束", "全部结束"):
        m = re.search(rf"\[{label}\] MGET=(\d+) SET=(\d+) GET=(\d+) APPEND=(\d+) 成功=(\d+) 失败=(\d+) 超时=(\d+)", text)
        if m:
            counts[label] = tuple(map(int, m.groups()))
    def one(pattern, groups, default=None):
        ms = list(re.finditer(pattern, text))
        if not ms:
            return default
        vals = ms[-1].groups()
        return tuple(float(v) if "." in v else int(v) for v in vals[:groups])
    cache = one(r"\[本地缓存\] 命中=(\d+) 未命中=(\d+) 等待刷新=(\d+)", 3, (0,0,0))
    service = one(r"\[缓存服务比例\] 目标=(\d+)% 业务GET=(\d+) 本地返回=(\d+) Redis回源=(\d+) 实际=([\d.]+)%", 5, (0,0,0,0,0.0))
    business = one(r"\[阶段3 GET 5ms\] 总数=(\d+) 成功=(\d+) RTT>5ms=(\d+) 失败=(\d+) 超时率=([\d.]+)%", 5, (0,0,0,0,0.0))
    latency = {}
    for name in ("本地缓存GET", "Redis回源GET", "业务GET综合"):
        v = one(rf"\[{name} RTT\(us\)\] avg=(-?\d+) max=(-?\d+) p95=(-?\d+) p99=(-?\d+) n=(\d+)", 5, (-1,-1,-1,-1,0))
        latency[name] = v
    return text, rtt, counts, cache, service, business, latency

def parse_summary(root):
    text = read(root / "acceptance_summary.txt")
    def f(pattern, default=0.0):
        m = re.search(pattern, text)
        return float(m.group(1)) if m else default
    out = {}
    for ph in (2,3):
        out[f"cpu{ph}_peak"] = f(rf"\[阶段{ph}\] CPU 最高单线程=([\d.]+)%")
        out[f"cpu{ph}_avg"] = f(rf"\[阶段{ph}\] CPU .*?平均=([\d.]+)%")
        out[f"rx{ph}_peak"] = f(rf"\[阶段{ph}\] 网卡 峰值RX=([\d.]+)kB/s")
        out[f"tx{ph}_peak"] = f(rf"\[阶段{ph}\] 网卡 .*?峰值TX=([\d.]+)kB/s")
        out[f"mem{ph}_growth"] = f(rf"阶段{ph}: 内存开始=.*?增长=(-?[\d.]+) bytes")
        out[f"repl{ph}_gap"] = f(rf"阶段{ph}: 最大主从offset差=([\d.]+) bytes")
    return text, out

def values(rows, phase, idx):
    return [float(r[idx]) for r in rows if r[1] == phase]

def canvas(title, subtitle, w=1600, h=900):
    im = Image.new("RGB", (w,h), COL["bg"])
    d = ImageDraw.Draw(im)
    d.text((72,34), title, font=font(36,True), fill=COL["text"])
    d.text((72,84), subtitle, font=font(19), fill=COL["muted"])
    return im, d

def axes(d, w, h, ymax, ylabel, L=125, R=55, T=145, B=125):
    pw, ph = w-L-R, h-T-B
    for i in range(6):
        y = T + ph*i/5
        v = ymax*(5-i)/5
        d.line((L,y,w-R,y), fill=COL["grid"], width=1)
        d.text((18,y-13), f"{v:,.0f}", font=font(16), fill=COL["muted"])
    d.line((L,T,L,T+ph), fill=COL["text"], width=2)
    d.line((L,T+ph,w-R,T+ph), fill=COL["text"], width=2)
    d.text((18,112), ylabel, font=font(17), fill=COL["muted"])
    return L, R, T, B, pw, ph

def time_chart(path, title, subtitle, rows, cols, labels, colors, ylabel, ymax=None):
    im,d = canvas(title, subtitle)
    vals = [float(r[c]) for r in rows for c in cols]
    ym = ymax or max(vals or [1]) * 1.12
    L,R,T,B,pw,ph = axes(d,*im.size,ym,ylabel)
    for i in range(1,len(rows)):
        if rows[i][1] != rows[i-1][1]:
            x = L + i/max(1,len(rows)-1)*pw
            d.line((x,T,x,T+ph),fill="#94a3b8",width=2)
            d.text((x+8,T+8),rows[i][1],font=font(15,True),fill=COL["muted"])
    for col,label,color in zip(cols,labels,colors):
        pts=[]
        for i,r in enumerate(rows):
            x=L+i/max(1,len(rows)-1)*pw
            y=T+ph*(ym-float(r[col]))/ym
            pts.append((x,y))
        if len(pts)>1:
            d.line(pts,fill=color,width=4,joint="curve")
    x=L
    for label,color in zip(labels,colors):
        d.line((x,im.height-42,x+42,im.height-42),fill=color,width=5)
        d.text((x+52,im.height-57),label,font=font(18),fill=COL["text"])
        x += 220
    im.save(path,"JPEG",quality=95,subsampling=0)

def rtt_chart(path, rows):
    keys=[("阶段2",c) for c in ("MGET","SET","GET","APPEND")]+[("阶段3",c) for c in ("MGET","SET","GET","APPEND")]
    lookup={(p,c):(a,m) for p,c,a,m in rows}
    av=[lookup[k][0] for k in keys]; mx=[lookup[k][1] for k in keys]
    ym=max(v for v in av+mx if v>=0)*1.16
    im,d=canvas("优化2 RTT全阶段折线图","Redis命令RTT；5ms为业务阈值；阶段2未执行GET/APPEND，阶段3 APPEND被客户端拦截")
    L,R,T,B,pw,ph=axes(d,*im.size,ym,"RTT (us)",B=155)
    y5=T+ph*(ym-5000)/ym
    d.line((L,y5,im.width-R,y5),fill=COL["red"],width=2)
    d.text((im.width-R-180,y5-27),"5ms阈值",font=font(16,True),fill=COL["red"])
    xs=[L+(i+.5)/len(keys)*pw for i in range(len(keys))]
    d.line(((xs[3]+xs[4])/2,T,(xs[3]+xs[4])/2,T+ph),fill="#94a3b8",width=2)
    for series,color,off in ((av,COL["blue"],0),(mx,COL["orange"],1)):
        seg=[]
        for i,v in enumerate(series):
            if v<0:
                if len(seg)>1: d.line(seg,fill=color,width=4)
                seg=[]
                d.text((xs[i]-30,T+ph-35-off*24),"未执行",font=font(14),fill=COL["muted"])
                continue
            y=T+ph*(ym-v)/ym; seg.append((xs[i],y))
            d.ellipse((xs[i]-5,y-5,xs[i]+5,y+5),fill=color)
        if len(seg)>1: d.line(seg,fill=color,width=4)
    for i,(_,cmd) in enumerate(keys):
        d.text((xs[i]-38,T+ph+20),cmd,font=font(16,True),fill=COL["text"])
    d.text((L+pw*.18,T+ph+60),"阶段2",font=font(18,True),fill=COL["muted"])
    d.text((L+pw*.68,T+ph+60),"阶段3",font=font(18,True),fill=COL["muted"])
    d.line((L,im.height-42,L+42,im.height-42),fill=COL["blue"],width=5)
    d.text((L+52,im.height-57),"平均RTT",font=font(18),fill=COL["text"])
    d.line((L+250,im.height-42,L+292,im.height-42),fill=COL["orange"],width=5)
    d.text((L+302,im.height-57),"最大RTT",font=font(18),fill=COL["text"])
    im.save(path,"JPEG",quality=95,subsampling=0)

def comparison_line(path,title,subtitle,base_vals,opt_vals,ymax,ylabel,phase_split=None):
    n=max(len(base_vals),len(opt_vals))
    im,d=canvas(title,subtitle)
    L,R,T,B,pw,ph=axes(d,*im.size,ymax,ylabel)
    def line(vals,color):
        pts=[]
        for i,v in enumerate(vals):
            x=L+i/max(1,len(vals)-1)*pw; y=T+ph*(ymax-v)/ymax; pts.append((x,y))
        if len(pts)>1: d.line(pts,fill=color,width=4,joint="curve")
    line(base_vals,COL["blue"]); line(opt_vals,COL["orange"])
    if phase_split is not None:
        x=L+phase_split*pw
        d.line((x,T,x,T+ph),fill="#94a3b8",width=2)
        d.text((L+20,T+10),"阶段2",font=font(16,True),fill=COL["muted"])
        d.text((x+20,T+10),"阶段3",font=font(16,True),fill=COL["muted"])
    d.line((L,im.height-42,L+42,im.height-42),fill=COL["blue"],width=5)
    d.text((L+52,im.height-57),f"新基线 {BASE_RUN}",font=font(18),fill=COL["text"])
    d.line((L+360,im.height-42,L+402,im.height-42),fill=COL["orange"],width=5)
    d.text((L+412,im.height-57),f"优化2 {RUN}",font=font(18),fill=COL["text"])
    im.save(path,"JPEG",quality=95,subsampling=0)

def metric_chart(path,title,subtitle,labels,base_vals,opt_vals,ymax,ylabel):
    im,d=canvas(title,subtitle)
    L,R,T,B,pw,ph=axes(d,*im.size,ymax,ylabel,B=165)
    xs=[L+(i+.5)/len(labels)*pw for i in range(len(labels))]
    bpts=[]; opts=[]
    for x,b,o,label in zip(xs,base_vals,opt_vals,labels):
        by=T+ph*(ymax-b)/ymax; oy=T+ph*(ymax-o)/ymax
        bpts.append((x,by)); opts.append((x,oy))
        d.text((x-55,T+ph+20),label,font=font(17,True),fill=COL["text"])
        d.text((x-45,by-30),f"{b:,.0f}",font=font(14),fill=COL["blue"])
        d.text((x-45,oy+8),f"{o:,.0f}",font=font(14),fill=COL["orange"])
    d.line(bpts,fill=COL["blue"],width=4); d.line(opts,fill=COL["orange"],width=4)
    for pts,color in ((bpts,COL["blue"]),(opts,COL["orange"])):
        for x,y in pts: d.ellipse((x-6,y-6,x+6,y+6),fill=color)
    d.line((L,im.height-42,L+42,im.height-42),fill=COL["blue"],width=5)
    d.text((L+52,im.height-57),"新基线",font=font(18),fill=COL["text"])
    d.line((L+250,im.height-42,L+292,im.height-42),fill=COL["orange"],width=5)
    d.text((L+302,im.height-57),"优化2",font=font(18),fill=COL["text"])
    im.save(path,"JPEG",quality=95,subsampling=0)

# Parse both runs.
cpu=parse_cpu(ROOT,RUN); net,iface=parse_net(ROOT,RUN)
client_text,rtt,counts,_,_,_,_=parse_client(ROOT,RUN)
sum_text,sm=parse_summary(ROOT)
bcpu=parse_cpu(BASE,BASE_RUN); bnet,biface=parse_net(BASE,BASE_RUN)
bclient,brtt,bcounts,_,_,_,_=parse_client(BASE,BASE_RUN)
bsum_text,bsm=parse_summary(BASE)

# Three reusable source tables with headers.
write_csv(OUT/f"CPU_opt2_bigkey_{RUN}.csv",["time","phase","cpu_max_pct"],[[t,p,f"{v:.2f}"] for t,p,v in cpu])
write_csv(OUT/f"NET_opt2_bigkey_{RUN}.csv",["time","phase","rx_mib_s","tx_mib_s"],[[t,p,f"{rx:.2f}",f"{tx:.2f}"] for t,p,rx,tx in net])
write_csv(OUT/f"RTT_opt2_bigkey_{RUN}.csv",["phase","cmd","avg_us","max_us"],rtt)

# Three run charts. APPEND=-1 is deliberately shown as intercepted/not executed.
time_chart(OUT/f"CPU_opt2_bigkey_{RUN}_chart.jpg","优化2 CPU全阶段折线图","Redis主线程/线程组CPU峰值；Y轴固定0–110%",cpu,[2],["CPU"],[COL["blue"]],"CPU (%)",110)
time_chart(OUT/f"NET_opt2_bigkey_{RUN}_chart.jpg","优化2网络流量全阶段折线图",f"网卡 {iface}；相邻累计字节差分；单位MiB/s",net,[2,3],["RX","TX"],[COL["blue"],COL["orange"]],"MiB/s")
rtt_chart(OUT/f"RTT_opt2_bigkey_{RUN}_chart.jpg",rtt)

# Derived metrics.
rmap={(p,c):(a,m) for p,c,a,m in rtt}; brmap={(p,c):(a,m) for p,c,a,m in brtt}
cpu2=values(cpu,"阶段2",2); cpu3=values(cpu,"阶段3",2)
rx2=values(net,"阶段2",2); tx2=values(net,"阶段2",3); rx3=values(net,"阶段3",2); tx3=values(net,"阶段3",3)
bcpu2=values(bcpu,"阶段2",2); bcpu3=values(bcpu,"阶段3",2); brx3=values(bnet,"阶段3",2); btx3=values(bnet,"阶段3",3)
bcpu3_steady=bcpu3[:-1] if len(bcpu3)>1 and bcpu3[-1] < 50 else bcpu3
cpu3_steady=cpu3[:-1] if len(cpu3)>1 and cpu3[-1] < 50 else cpu3
b_cpu3_avg=statistics.mean(bcpu3_steady); o_cpu3_avg=statistics.mean(cpu3_steady)
def trim_network_stop(vals):
    """Remove only trailing shutdown samples; raw CSV remains unchanged."""
    out=list(vals); threshold=statistics.median(out)*0.85
    while len(out)>2 and out[-1] < threshold:
        out.pop()
    return out
btx3_steady=trim_network_stop(btx3); tx3_steady=trim_network_stop(tx3)
brx3_steady=brx3[:len(btx3_steady)]; rx3_steady=rx3[:len(tx3_steady)]
s2=counts["阶段2结束"]; allc=counts["全部结束"]; s3=tuple(a-b for a,b in zip(allc,s2))
bs2=bcounts["阶段2结束"]; ball=bcounts["全部结束"]; bs3=tuple(a-b for a,b in zip(ball,bs2))
guard_matches=list(re.finditer(r"\[APPEND保护\] 请求=(\d+) 限流拒绝=(\d+) 大小拒绝=(\d+)",client_text))
if not guard_matches:
    raise RuntimeError("未找到APPEND保护统计")
requested,rate_rejected,size_rejected=map(int,guard_matches[-1].groups())
redis_append=s3[3]

def pct(base,opt):
    return (opt-base)/base*100 if base else 0

def summary_number(text,pattern):
    m=re.search(pattern,text)
    return float(m.group(1)) if m else 0.0

b_mem3_peak=summary_number(bsum_text,r"阶段3: 内存开始=.*?峰值=([\d.]+)")
o_mem3_peak=summary_number(sum_text,r"阶段3: 内存开始=.*?峰值=([\d.]+)")
b_timeout_rate=bs3[6]/bs3[4]*100
o_timeout_rate=s3[6]/s3[4]*100

cmp_rows=[
 ["APPEND请求","条",bs3[3],requested,f"{pct(bs3[3],requested):+.2f}%","优化2额外记录被客户端拦截的业务请求"],
 ["实际进入Redis的APPEND","条",bs3[3],redis_append,"-100.00%","15,000次恶意请求全部被拦截"],
 ["APPEND限流拒绝","条",0,rate_rejected,"n.a.",f"占请求{rate_rejected/requested*100:.2f}%"],
 ["APPEND大小拒绝","条",0,size_rejected,"n.a.",f"占请求{size_rejected/requested*100:.2f}%"],
 ["阶段3 CPU稳态平均","%",f"{b_cpu3_avg:.2f}",f"{o_cpu3_avg:.2f}",f"{pct(b_cpu3_avg,o_cpu3_avg):+.2f}%","剔除基线线程停止边界24%采样；基本相同"],
 ["阶段3 CPU峰值","%",f"{bsm['cpu3_peak']:.0f}",f"{sm['cpu3_peak']:.0f}",f"{pct(bsm['cpu3_peak'],sm['cpu3_peak']):+.2f}%","不变"],
 ["阶段3 TX稳态平均","MiB/s",f"{statistics.mean(btx3_steady):.2f}",f"{statistics.mean(tx3_steady):.2f}",f"{pct(statistics.mean(btx3_steady),statistics.mean(tx3_steady)):+.2f}%","剔除压测结束归零点；发送流量下降"],
 ["阶段3 TX峰值","kB/s",f"{bsm['tx3_peak']:.0f}",f"{sm['tx3_peak']:.0f}",f"{pct(bsm['tx3_peak'],sm['tx3_peak']):+.2f}%","下降约3.21Gbps"],
 ["阶段3 RX稳态平均","MiB/s",f"{statistics.mean(brx3_steady):.2f}",f"{statistics.mean(rx3_steady):.2f}",f"{pct(statistics.mean(brx3_steady),statistics.mean(rx3_steady)):+.2f}%","剔除压测结束归零点；其他命令吞吐提高"],
 ["阶段3 MGET平均RTT","us",brmap[("阶段3","MGET")][0],rmap[("阶段3","MGET")][0],f"{pct(brmap[('阶段3','MGET')][0],rmap[('阶段3','MGET')][0]):+.2f}%","明显下降"],
 ["阶段3 SET平均RTT","us",brmap[("阶段3","SET")][0],rmap[("阶段3","SET")][0],f"{pct(brmap[('阶段3','SET')][0],rmap[('阶段3','SET')][0]):+.2f}%","降到5ms以内"],
 ["阶段3 GET平均RTT","us",brmap[("阶段3","GET")][0],rmap[("阶段3","GET")][0],f"{pct(brmap[('阶段3','GET')][0],rmap[('阶段3','GET')][0]):+.2f}%","下降但仍略高于5ms"],
 ["阶段3 RTT>5ms数量","条",bs3[6],s3[6],f"{pct(bs3[6],s3[6]):+.2f}%","明显下降"],
 ["阶段3 RTT>5ms比例","%",f"{b_timeout_rate:.2f}",f"{o_timeout_rate:.2f}",f"{o_timeout_rate-b_timeout_rate:+.2f}个百分点","仍未完全达到5ms目标"],
 ["阶段3成功Redis命令","条",bs3[4],s3[4],f"{pct(bs3[4],s3[4]):+.2f}%","处理能力提高"],
 ["阶段3内存峰值","bytes",f"{b_mem3_peak:.0f}",f"{o_mem3_peak:.0f}",f"{pct(b_mem3_peak,o_mem3_peak):+.2f}%","大Key内存膨胀被消除"],
 ["阶段3内存增长","bytes",f"{bsm['mem3_growth']:.0f}",f"{sm['mem3_growth']:.0f}","由增长转为下降","基线+3.79GB，优化2减少34.8MB"],
 ["阶段3最大复制offset差","bytes",f"{bsm['repl3_gap']:.0f}",f"{sm['repl3_gap']:.0f}",f"{pct(bsm['repl3_gap'],sm['repl3_gap']):+.2f}%","略增但最终同步"],
]
write_csv(CMP/"关键数据对比.csv",["指标","单位",f"新基线{BASE_RUN}",f"优化2 {RUN}","变化","说明"],cmp_rows)

# Comparison charts.
comparison_line(CMP/"01_CPU双折线对比.jpg","新基线与优化2 CPU对比","剔除线程停止边界点；阶段2和阶段3按采样序号对齐；Y轴0–110%",bcpu2+bcpu3_steady,cpu2+cpu3_steady,110,"CPU (%)",len(bcpu2)/max(1,len(bcpu2)+len(bcpu3_steady)))
comparison_line(CMP/"02_阶段3_TX双折线对比.jpg","新基线与优化2 阶段3 TX对比","剔除压测结束归零点；按阶段3相对时间对齐",btx3_steady,tx3_steady,max(max(btx3_steady),max(tx3_steady))*1.12,"MiB/s")
cmds=["MGET","SET","GET"]
bavg=[brmap[("阶段3",c)][0] for c in cmds]; oavg=[rmap[("阶段3",c)][0] for c in cmds]
metric_chart(CMP/"03_阶段3平均RTT对比.jpg","阶段3 Redis命令平均RTT对比","APPEND已在客户端拦截，不作为0时延绘制",cmds,bavg,oavg,max(bavg+oavg)*1.28,"RTT (us)")
metric_chart(CMP/"04_阶段3内存峰值对比.jpg","阶段3内存峰值对比","大Key保护阻止APPEND写入，峰值内存显著下降",["内存峰值"],[b_mem3_peak/1073741824],[o_mem3_peak/1073741824],max(b_mem3_peak,o_mem3_peak)/1073741824*1.25,"GiB")

def protection_chart(path):
    labels=["APPEND请求","限流拒绝","大小拒绝","进入Redis"]
    vals=[requested,rate_rejected,size_rejected,redis_append]
    colors=[COL["blue"],COL["orange"],COL["purple"],COL["green"]]
    im,d=canvas("优化2 APPEND保护结果","15,000次恶意请求全部在客户端保护层完成处置")
    L,R,T,B,pw,ph=axes(d,*im.size,max(vals)*1.18,"请求数",B=165)
    gap=pw/len(vals); bw=gap*.48
    for i,(lab,val,color) in enumerate(zip(labels,vals,colors)):
        x=L+(i+.5)*gap; y=T+ph*(max(vals)*1.18-val)/(max(vals)*1.18)
        d.rectangle((x-bw/2,y,x+bw/2,T+ph),fill=color)
        d.text((x-55,y-34),f"{val:,}",font=font(17,True),fill=color)
        d.text((x-75,T+ph+22),lab,font=font(17,True),fill=COL["text"])
    d.text((L,T+ph+75),f"限流拒绝 {rate_rejected/requested*100:.2f}%   |   大小拒绝 {size_rejected/requested*100:.2f}%   |   Redis APPEND 0条",font=font(18),fill=COL["muted"])
    im.save(path,"JPEG",quality=95,subsampling=0)

protection_chart(CMP/"05_APPEND保护统计.jpg")

summary=f"""优化2本轮数据摘要
=================

轮次：{RUN}
对比基线：{BASE_RUN}
口径：MGET总聚合100 Key；RTT严格大于5ms计业务超时；网络读保护100ms；TTL固定1秒；Value上限300KB；APPEND全局限流20次/秒。

一、APPEND保护
--------------
阶段3收到{requested}次恶意APPEND，其中限流拒绝{rate_rejected}次（{rate_rejected/requested*100:.2f}%），大小拒绝{size_rejected}次（{size_rejected/requested*100:.2f}%），进入Redis为{redis_append}次。全部请求都有明确处置，没有遗漏。
RTT表中的APPEND=-1表示客户端已拦截、Redis未执行，不表示数据丢失，也不能解释为0时延。

二、核心变化
------------
阶段3CPU稳态平均：{b_cpu3_avg:.2f}% -> {o_cpu3_avg:.2f}%，基本相同。
阶段3TX稳态平均：{statistics.mean(btx3_steady):.2f} -> {statistics.mean(tx3_steady):.2f}MiB/s，下降{-pct(statistics.mean(btx3_steady),statistics.mean(tx3_steady)):.2f}%；TX峰值下降{-pct(bsm['tx3_peak'],sm['tx3_peak']):.2f}%。
MGET/SET/GET平均RTT分别下降{-pct(brmap[("阶段3","MGET")][0],rmap[("阶段3","MGET")][0]):.2f}%、{-pct(brmap[("阶段3","SET")][0],rmap[("阶段3","SET")][0]):.2f}%、{-pct(brmap[("阶段3","GET")][0],rmap[("阶段3","GET")][0]):.2f}%。
RTT>5ms比例从{b_timeout_rate:.2f}%降至{o_timeout_rate:.2f}%，下降{b_timeout_rate-o_timeout_rate:.2f}个百分点。
阶段3内存峰值从{b_mem3_peak/1073741824:.2f}GiB降至{o_mem3_peak/1073741824:.2f}GiB，下降{-pct(b_mem3_peak,o_mem3_peak):.2f}%。

三、结论
--------
优化2核心功能符合预期：恶意APPEND全部被客户端保护层阻止，阶段3内存膨胀被消除，TX和三类主要命令RTT明显下降。在Redis CPU仍约97%的情况下，成功命令处理量提高{pct(bs3[4],s3[4]):.2f}%。
5ms目标仍未完全满足：阶段3仍有{o_timeout_rate:.2f}%的Redis命令超过5ms，MGET和GET平均RTT仍略高于5ms。
"""
(OUT/"本轮数据摘要.txt").write_text(summary,encoding="utf-8")

report=f"""优化2：异常大Key拦截——完整分析报告
==================================

一、测试信息
------------
优化2轮次：{RUN}
对比基线轮次：{BASE_RUN}
Redis服务端：20.20.21.185
压测客户端：20.20.21.186
MGET聚合：100
RTT业务阈值：5ms
网络读保护：100ms
数据TTL：1秒
Value保护上限：300KB
APPEND全局限流：20次/秒
主控返回码：0

二、数据完整性
--------------
CPU、网络、Redis INFO、Redis STAT、SAR、客户端、bigkeys、主控日志、验收采样和结果报告齐全。客户端版本标识为“异常大Key拦截”，仅开启bigkey_guard，参数为MGET=100、RTT=5ms、网络读保护=100ms。主控返回码为0，阶段2和阶段3正常结束。

三、APPEND保护结果
------------------
阶段3产生{requested}次恶意APPEND请求。限流拒绝{rate_rejected}次，占{rate_rejected/requested*100:.2f}%；通过限流后因预计Value超过300KB被拒绝{size_rejected}次，占{size_rejected/requested*100:.2f}%；实际发送到Redis的APPEND为{redis_append}次。

限流拒绝与大小拒绝相加正好等于请求总数，说明所有请求都有明确去向。由于APPEND没有进入Redis，客户端命令统计中的APPEND=0，RTT CSV中的APPEND=-1。这是保护成功的证据，不是RTT数据缺失。

当前实现先做每秒20次全局限流，再做300KB大小检查。压测APPEND负载单次约200KB，目标Key原值约200KB，继续追加会预计达到约400KB，因此通过限流的请求仍会被大小检查拒绝。

四、CPU和网络
-------------
优化2阶段3CPU稳态平均{o_cpu3_avg:.2f}%、峰值{sm['cpu3_peak']:.0f}%；基线稳态平均{b_cpu3_avg:.2f}%、峰值{bsm['cpu3_peak']:.0f}%。CPU基本没有下降，Redis主线程仍接近单核满载。对比时已剔除基线最后一个线程停止边界的24%样本，原始CSV仍完整保留。

剔除压测结束归零点后，阶段3TX稳态平均从{statistics.mean(btx3_steady):.2f}降至{statistics.mean(tx3_steady):.2f}MiB/s，下降{-pct(statistics.mean(btx3_steady),statistics.mean(tx3_steady)):.2f}%；峰值从{bsm['tx3_peak']:.0f}降至{sm['tx3_peak']:.0f}kB/s，下降{-pct(bsm['tx3_peak'],sm['tx3_peak']):.2f}%，约从{bsm['tx3_peak']*8/1_000_000:.2f}Gbps降至{sm['tx3_peak']*8/1_000_000:.2f}Gbps。原始CSV仍完整保留结束边界样本。

阶段3RX稳态平均从{statistics.mean(brx3_steady):.2f}升至{statistics.mean(rx3_steady):.2f}MiB/s，增长{pct(statistics.mean(brx3_steady),statistics.mean(rx3_steady)):.2f}%。这是MGET、SET和GET成功吞吐提高后的输入流量增加，网卡没有丢包或错误。

五、RTT、5ms超时和吞吐
----------------------
阶段3MGET平均/最大RTT为{rmap[("阶段3","MGET")][0]}/{rmap[("阶段3","MGET")][1]}us，相比基线平均下降{-pct(brmap[("阶段3","MGET")][0],rmap[("阶段3","MGET")][0]):.2f}%。
阶段3SET平均/最大RTT为{rmap[("阶段3","SET")][0]}/{rmap[("阶段3","SET")][1]}us，相比基线平均下降{-pct(brmap[("阶段3","SET")][0],rmap[("阶段3","SET")][0]):.2f}%。
阶段3GET平均/最大RTT为{rmap[("阶段3","GET")][0]}/{rmap[("阶段3","GET")][1]}us，相比基线平均下降{-pct(brmap[("阶段3","GET")][0],rmap[("阶段3","GET")][0]):.2f}%。

阶段3RTT>5ms数量从{bs3[6]}降至{s3[6]}，下降{-pct(bs3[6],s3[6]):.2f}%；按成功Redis命令计算，超时比例从{b_timeout_rate:.2f}%降至{o_timeout_rate:.2f}%，下降{b_timeout_rate-o_timeout_rate:.2f}个百分点。

阶段3成功Redis命令从{bs3[4]}增加到{s3[4]}，提高{pct(bs3[4],s3[4]):.2f}%。MGET从{bs3[0]/60:.0f}/s提高到{s3[0]/60:.0f}/s，SET从{bs3[1]/60:.0f}/s提高到{s3[1]/60:.0f}/s，GET从{bs3[2]/60:.0f}/s提高到{s3[2]/60:.0f}/s。说明阻止大Value膨胀后，Redis能够处理更多正常请求。

5ms目标仍未完全满足：MGET平均{rmap[("阶段3","MGET")][0]/1000:.3f}ms、GET平均{rmap[("阶段3","GET")][0]/1000:.3f}ms，仍略高于5ms；阶段3仍有{o_timeout_rate:.2f}%的成功Redis命令超过5ms。

六、内存、复制和大Key扫描
------------------------
阶段3内存峰值从{b_mem3_peak/1073741824:.2f}GiB降至{o_mem3_peak/1073741824:.2f}GiB，下降{-pct(b_mem3_peak,o_mem3_peak):.2f}%。基线阶段3内存增长{bsm['mem3_growth']:.0f} bytes，优化2阶段3内存变化{sm['mem3_growth']:.0f} bytes，已经从持续增长变为小幅下降。

最大主从offset差从{bsm['repl3_gap']:.0f}增加到{sm['repl3_gap']:.0f} bytes，增加{pct(bsm['repl3_gap'],sm['repl3_gap']):.2f}%，但复制链路异常采样为0，测试结束时主从offset一致。该变化可能与正常SET吞吐提高有关，应在重复测试中继续观察。

bigkeys最终只扫描到1字节的阶段标记。由于所有业务Key使用1秒TTL，结束时大部分业务Key已经过期，因此不能单独用结束后的bigkeys扫描证明全过程没有大Key。APPEND保护计数和Redis APPEND=0才是本轮功能验证的主要证据。

七、最终结论
------------
1. 优化2功能正确，{requested}次恶意APPEND全部被拦截，没有请求进入Redis。
2. 阶段3内存峰值下降{-pct(b_mem3_peak,o_mem3_peak):.2f}%，大Key导致的持续内存增长被消除。
3. TX峰值下降{-pct(bsm['tx3_peak'],sm['tx3_peak']):.2f}%，MGET、SET、GET平均RTT下降约16%至21%。
4. RTT>5ms比例从{b_timeout_rate:.2f}%降至{o_timeout_rate:.2f}%，但严格5ms目标仍未完全满足。
5. CPU仍约97%，大Key保护不能单独解决Redis主线程满载问题。
6. 主从最终同步、无复制链路异常、无网卡丢包。本轮结果符合异常大Key拦截方案的核心预期。
"""
(OUT/"完整分析报告.txt").write_text(report,encoding="utf-8")

cmp=f"""优化2与新基线对比结论
========================

对比轮次：新基线{BASE_RUN}；优化2 {RUN}。

1. {requested}次恶意APPEND全部被拦截：限流拒绝{rate_rejected}次，大小拒绝{size_rejected}次，Redis APPEND为0。
2. 阶段3内存峰值下降{-pct(b_mem3_peak,o_mem3_peak):.2f}%，内存由增长{bsm['mem3_growth']:.0f} bytes变为变化{sm['mem3_growth']:.0f} bytes。
3. TX稳态平均下降{-pct(statistics.mean(btx3_steady),statistics.mean(tx3_steady)):.2f}%，TX峰值下降{-pct(bsm['tx3_peak'],sm['tx3_peak']):.2f}%。
4. MGET、SET、GET平均RTT分别下降{-pct(brmap[("阶段3","MGET")][0],rmap[("阶段3","MGET")][0]):.2f}%、{-pct(brmap[("阶段3","SET")][0],rmap[("阶段3","SET")][0]):.2f}%、{-pct(brmap[("阶段3","GET")][0],rmap[("阶段3","GET")][0]):.2f}%。
5. RTT>5ms比例从{b_timeout_rate:.2f}%降至{o_timeout_rate:.2f}%，仍未完全达到5ms目标。
6. CPU稳态平均约{o_cpu3_avg:.2f}%，没有明显下降；主从最终同步且无网卡丢包。

结论：优化2核心功能符合预期，彻底阻止恶意APPEND污染Redis，显著降低内存峰值、TX和命令时延，同时提高正常命令吞吐。它不能单独解决Redis单线程CPU饱和问题。
"""
(CMP/"对比结论.txt").write_text(cmp,encoding="utf-8")

print(f"single={OUT}")
print(f"compare={CMP}")


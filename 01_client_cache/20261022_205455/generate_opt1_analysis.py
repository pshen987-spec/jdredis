#!/usr/bin/env python3
import csv
import re
import statistics
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

RUN = "20261022_205455"
BASE_RUN = "20261022_195613"
ROOT = Path(__file__).resolve().parent
BASE = ROOT.parents[1] / "00_baseline" / BASE_RUN
OUT = ROOT / f"opt1_cache_{RUN}数据"
CMP = ROOT / "comparison_new_baseline_vs_opt1"
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
    im,d=canvas("优化1 RTT全阶段折线图","Redis命令RTT；5ms为业务阈值；阶段2未执行GET/APPEND")
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
    d.text((L+412,im.height-57),f"优化1 {RUN}",font=font(18),fill=COL["text"])
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
    d.text((L+302,im.height-57),"优化1",font=font(18),fill=COL["text"])
    im.save(path,"JPEG",quality=95,subsampling=0)

# Parse both runs.
cpu=parse_cpu(ROOT,RUN); net,iface=parse_net(ROOT,RUN)
client_text,rtt,counts,cache,service,business,latency=parse_client(ROOT,RUN)
sum_text,sm=parse_summary(ROOT)
bcpu=parse_cpu(BASE,BASE_RUN); bnet,biface=parse_net(BASE,BASE_RUN)
bclient,brtt,bcounts,_,_,_,_=parse_client(BASE,BASE_RUN)
bsum_text,bsm=parse_summary(BASE)

# Three reusable source tables with headers.
write_csv(OUT/f"CPU_opt1_cache_{RUN}.csv",["time","phase","cpu_max_pct"],[[t,p,f"{v:.2f}"] for t,p,v in cpu])
write_csv(OUT/f"NET_opt1_cache_{RUN}.csv",["time","phase","rx_mib_s","tx_mib_s"],[[t,p,f"{rx:.2f}",f"{tx:.2f}"] for t,p,rx,tx in net])
write_csv(OUT/f"RTT_opt1_cache_{RUN}.csv",["phase","cmd","avg_us","max_us"],rtt)

# Three run charts.
time_chart(OUT/f"CPU_opt1_cache_{RUN}_chart.jpg","优化1 CPU全阶段折线图","Redis主线程/线程组CPU峰值；Y轴固定0–110%",cpu,[2],["CPU"],[COL["blue"]],"CPU (%)",110)
time_chart(OUT/f"NET_opt1_cache_{RUN}_chart.jpg","优化1网络流量全阶段折线图",f"网卡 {iface}；相邻累计字节差分；单位MiB/s",net,[2,3],["RX","TX"],[COL["blue"],COL["orange"]],"MiB/s")
rtt_chart(OUT/f"RTT_opt1_cache_{RUN}_chart.jpg",rtt)

# Derived data.
rmap={(p,c):(a,m) for p,c,a,m in rtt}; brmap={(p,c):(a,m) for p,c,a,m in brtt}
cpu2=values(cpu,"阶段2",2); cpu3=values(cpu,"阶段3",2)
rx2=values(net,"阶段2",2); tx2=values(net,"阶段2",3); rx3=values(net,"阶段3",2); tx3=values(net,"阶段3",3)
bcpu2=values(bcpu,"阶段2",2); bcpu3=values(bcpu,"阶段3",2); btx3=values(bnet,"阶段3",3)
# 基线阶段3最后一个24%点发生在线程停止边界；保留在原始CSV，正式对比剔除该边界点。
bcpu3_steady=bcpu3[:-1] if len(bcpu3)>1 and bcpu3[-1] < 50 else bcpu3
cpu3_steady=cpu3[:-1] if len(cpu3)>1 and cpu3[-1] < 50 else cpu3
b_cpu3_steady_avg=statistics.mean(bcpu3_steady)
o_cpu3_steady_avg=statistics.mean(cpu3_steady)
s2=counts["阶段2结束"]; allc=counts["全部结束"]
bs2=bcounts["阶段2结束"]; ball=bcounts["全部结束"]
s3=tuple(a-b for a,b in zip(allc,s2)); bs3=tuple(a-b for a,b in zip(ball,bs2))
hit,miss,wait=cache; target,biz_total,local_total,origin_total,actual=service
biz_n,biz_ok,biz_timeout,biz_fail,biz_timeout_rate=business
local_lat=latency["本地缓存GET"]; origin_lat=latency["Redis回源GET"]; biz_lat=latency["业务GET综合"]
base_get=bs3[2]; base_get_qps=base_get/60; biz_qps=biz_total/60; origin_qps=origin_total/60

def pct(base,opt):
    return (opt-base)/base*100 if base else 0

cmp_rows=[
 ["阶段2 CPU平均","%",f"{bsm['cpu2_avg']:.2f}",f"{sm['cpu2_avg']:.2f}",f"{pct(bsm['cpu2_avg'],sm['cpu2_avg']):+.2f}%","缓存尚未参与阶段2，变化主要是运行波动"],
 ["阶段3 CPU稳态平均","%",f"{b_cpu3_steady_avg:.2f}",f"{o_cpu3_steady_avg:.2f}",f"{pct(b_cpu3_steady_avg,o_cpu3_steady_avg):+.2f}%","剔除基线线程停止边界24%采样；基本相同"],
 ["阶段3 CPU峰值","%",f"{bsm['cpu3_peak']:.0f}",f"{sm['cpu3_peak']:.0f}",f"{pct(bsm['cpu3_peak'],sm['cpu3_peak']):+.2f}%","基本相同"],
 ["阶段3 TX峰值","kB/s",f"{bsm['tx3_peak']:.0f}",f"{sm['tx3_peak']:.0f}",f"{pct(bsm['tx3_peak'],sm['tx3_peak']):+.2f}%","未降低，仍接近20Gbps"],
 ["阶段3 RX峰值","kB/s",f"{bsm['rx3_peak']:.0f}",f"{sm['rx3_peak']:.0f}",f"{pct(bsm['rx3_peak'],sm['rx3_peak']):+.2f}%","基本相同"],
 ["阶段3 MGET平均RTT","us",brmap[("阶段3","MGET")][0],rmap[("阶段3","MGET")][0],f"{pct(brmap[('阶段3','MGET')][0],rmap[('阶段3','MGET')][0]):+.2f}%","小幅下降"],
 ["阶段3 SET平均RTT","us",brmap[("阶段3","SET")][0],rmap[("阶段3","SET")][0],f"{pct(brmap[('阶段3','SET')][0],rmap[('阶段3','SET')][0]):+.2f}%","小幅下降"],
 ["阶段3 Redis回源GET平均RTT","us",brmap[("阶段3","GET")][0],rmap[("阶段3","GET")][0],f"{pct(brmap[('阶段3','GET')][0],rmap[('阶段3','GET')][0]):+.2f}%","回源请求仍受Redis饱和影响"],
 ["阶段3 APPEND平均RTT","us",brmap[("阶段3","APPEND")][0],rmap[("阶段3","APPEND")][0],f"{pct(brmap[('阶段3','APPEND')][0],rmap[('阶段3','APPEND')][0]):+.2f}%","小幅下降"],
 ["业务GET综合平均RTT","us",brmap[("阶段3","GET")][0],biz_lat[0],f"{pct(brmap[('阶段3','GET')][0],biz_lat[0]):+.2f}%","包含本地返回，用户侧显著改善"],
 ["阶段3业务GET数量","条",base_get,biz_total,f"{pct(base_get,biz_total):+.2f}%","同一60秒内业务处理量约5.05倍"],
 ["阶段3Redis回源GET数量","条",base_get,origin_total,f"{pct(base_get,origin_total):+.2f}%","绝对回源量没有下降"],
 ["阶段3 RTT>5ms总数","条",bs3[6],s3[6],f"{pct(bs3[6],s3[6]):+.2f}%","仅统计Redis命令；不含本地返回"],
 ["阶段3内存增长","bytes",f"{bsm['mem3_growth']:.0f}",f"{sm['mem3_growth']:.0f}",f"{pct(bsm['mem3_growth'],sm['mem3_growth']):+.2f}%","基本相同"],
 ["阶段3最大复制offset差","bytes",f"{bsm['repl3_gap']:.0f}",f"{sm['repl3_gap']:.0f}",f"{pct(bsm['repl3_gap'],sm['repl3_gap']):+.2f}%","增大但链路正常且最终追平"],
]
write_csv(CMP/"关键数据对比.csv",["指标","单位",f"新基线{BASE_RUN}",f"优化1 {RUN}","变化百分比","说明"],cmp_rows)

# Comparison charts.
comparison_line(CMP/"01_CPU双折线对比.jpg","新基线与优化1 CPU对比","剔除线程停止边界点；阶段2和阶段3按采样序号对齐；Y轴0–110%",bcpu2+bcpu3_steady,cpu2+cpu3_steady,110,"CPU (%)",len(bcpu2)/max(1,len(bcpu2)+len(bcpu3_steady)))
comparison_line(CMP/"02_阶段3_TX双折线对比.jpg","新基线与优化1 阶段3 TX对比","按阶段3相对时间对齐；优化1峰值未下降",btx3,tx3,max(max(btx3),max(tx3))*1.12,"MiB/s")
cmds=["MGET","SET","GET回源","APPEND"]
bavg=[brmap[("阶段3",c)][0] for c in ("MGET","SET","GET","APPEND")]
oavg=[rmap[("阶段3",c)][0] for c in ("MGET","SET","GET","APPEND")]
metric_chart(CMP/"03_阶段3Redis命令平均RTT对比.jpg","阶段3 Redis命令平均RTT对比","缓存命中请求不在此图；四类Redis命令时延仅小幅变化",cmds,bavg,oavg,max(bavg+oavg)*1.30,"RTT (us)")
metric_chart(CMP/"04_业务GET平均RTT对比.jpg","业务GET综合平均RTT对比","新基线全部访问Redis；优化1包含80%本地返回",["业务GET"],[brmap[("阶段3","GET")][0]],[biz_lat[0]],brmap[("阶段3","GET")][0]*1.25,"RTT (us)")

summary=f"""优化1本轮数据摘要
=================

轮次：{RUN}
对比基线：{BASE_RUN}
口径：MGET总聚合100 Key；RTT严格大于5ms计业务超时；网络读保护100ms；TTL固定1秒；本地缓存TTL 500ms；缓存服务目标80%。

一、缓存与业务GET
----------------
业务GET共{biz_total}次，本地返回{local_total}次，Redis回源{origin_total}次，实际本地服务比例{actual:.4f}%。
本地缓存命中{hit}次、未命中{miss}次、等待刷新{wait}次。
业务GET综合平均RTT={biz_lat[0]}us、最大={biz_lat[1]}us、P95={biz_lat[2]}us、P99={biz_lat[3]}us。
业务GET RTT>5ms共{biz_timeout}次，超时率{biz_timeout_rate:.4f}%，失败{biz_fail}次。

二、与基线的主要变化
--------------------
业务GET吞吐从{base_get_qps:.1f}/s提升到{biz_qps:.1f}/s，达到{biz_qps/base_get_qps:.2f}倍。
业务GET综合平均RTT从{brmap[("阶段3","GET")][0]}us降至{biz_lat[0]}us，下降{-pct(brmap[("阶段3","GET")][0],biz_lat[0]):.2f}%。
Redis回源GET为{origin_qps:.1f}/s，基线为{base_get_qps:.1f}/s，绝对回源量变化{pct(base_get,origin_total):+.2f}%。
阶段3 TX峰值从{bsm['tx3_peak']:.0f}升至{sm['tx3_peak']:.0f}kB/s，变化{pct(bsm['tx3_peak'],sm['tx3_peak']):+.2f}%。
阶段3 CPU稳态平均从{b_cpu3_steady_avg:.2f}%变为{o_cpu3_steady_avg:.2f}%，增加{o_cpu3_steady_avg-b_cpu3_steady_avg:.2f}个百分点，基本相同。

三、结论
--------
缓存功能和80%服务比例均正确生效，用户侧平均时延下降约80%，同一60秒内业务GET处理能力提升约5倍。
Redis侧没有减压：回源GET绝对数量、CPU和TX均未下降。原因是压测线程没有固定业务QPS；本地请求完成得更快后立即继续发送，放大了总业务量，20%的回源仍约等于基线全部GET流量。
本轮证明的是“同等Redis压力下业务容量提高约5倍”，不能证明“相同业务流量下Redis网卡下降80%”。
"""
(OUT/"本轮数据摘要.txt").write_text(summary,encoding="utf-8")

report=f"""优化1：客户端缓存——完整分析报告
================================

一、测试信息
------------
优化1轮次：{RUN}
对比基线轮次：{BASE_RUN}
Redis服务端：20.20.21.185
压测客户端：20.20.21.186
MGET聚合：100
RTT业务阈值：5ms
网络读保护：100ms
Redis数据TTL：1秒
缓存服务目标：80%
本地缓存TTL：500ms
主控返回码：0

二、数据完整性
--------------
本轮CPU、网络、Redis INFO、Redis STAT、SAR、客户端、bigkeys、主控日志、验收采样和结果报告齐全。客户端版本标识为“客户端多级缓存”，MGET=100、RTT阈值=5ms、网络读保护=100ms。阶段2和阶段3均正常结束，客户端失败数为0。

三、缓存功能与业务GET
--------------------
阶段3业务GET共{biz_total}次，其中本地返回{local_total}次，Redis回源{origin_total}次，实际本地服务比例{actual:.4f}%，与80%目标一致。本地缓存计数为命中{hit}次、未命中{miss}次、等待刷新{wait}次。

本地缓存GET平均RTT={local_lat[0]}us、最大={local_lat[1]}us、P95={local_lat[2]}us、P99={local_lat[3]}us。Redis回源GET平均RTT={origin_lat[0]}us、最大={origin_lat[1]}us、P95={origin_lat[2]}us、P99={origin_lat[3]}us。合并两种路径后，业务GET平均RTT={biz_lat[0]}us、最大={biz_lat[1]}us、P95={biz_lat[2]}us、P99={biz_lat[3]}us。

业务GET RTT>5ms共{biz_timeout}次，超时率{biz_timeout_rate:.4f}%，即{100-biz_timeout_rate:.4f}%的业务GET在5ms内完成。由于P95为{biz_lat[2]}us，当前结果仍未达到“至少95%的GET低于5ms”。

四、CPU和网络
-------------
阶段2CPU平均{sm['cpu2_avg']:.2f}%、峰值{sm['cpu2_peak']:.0f}%；阶段3CPU稳态平均{o_cpu3_steady_avg:.2f}%、峰值{sm['cpu3_peak']:.0f}%。新基线阶段3最后一个24%样本发生在线程停止边界，正式对比将其剔除；剔除后基线稳态平均为{b_cpu3_steady_avg:.2f}%，优化1仅增加{o_cpu3_steady_avg-b_cpu3_steady_avg:.2f}个百分点，CPU基本没有改善，主线程仍接近单核满载。原始CSV仍完整保留该边界采样。

阶段3RX峰值{sm['rx3_peak']:.0f}kB/s，TX峰值{sm['tx3_peak']:.0f}kB/s，TX折算约{sm['tx3_peak']*8/1_000_000:.2f}Gbps。基线TX峰值为{bsm['tx3_peak']:.0f}kB/s，优化1变化{pct(bsm['tx3_peak'],sm['tx3_peak']):+.2f}%，没有出现预期的网卡下降。

五、Redis命令RTT和总超时
-----------------------
阶段3MGET平均/最大RTT为{rmap[("阶段3","MGET")][0]}/{rmap[("阶段3","MGET")][1]}us；SET为{rmap[("阶段3","SET")][0]}/{rmap[("阶段3","SET")][1]}us；Redis回源GET为{rmap[("阶段3","GET")][0]}/{rmap[("阶段3","GET")][1]}us；APPEND为{rmap[("阶段3","APPEND")][0]}/{rmap[("阶段3","APPEND")][1]}us。

四类Redis命令平均RTT相对基线分别变化：MGET {pct(brmap[("阶段3","MGET")][0],rmap[("阶段3","MGET")][0]):+.2f}%，SET {pct(brmap[("阶段3","SET")][0],rmap[("阶段3","SET")][0]):+.2f}%，GET {pct(brmap[("阶段3","GET")][0],rmap[("阶段3","GET")][0]):+.2f}%，APPEND {pct(brmap[("阶段3","APPEND")][0],rmap[("阶段3","APPEND")][0]):+.2f}%。说明Redis自身排队和处理时延只小幅改善。

阶段3Redis命令RTT>5ms从基线{bs3[6]}条降到{s3[6]}条，仅下降{-pct(bs3[6],s3[6]):.2f}%。这个总数不包含本地缓存直接返回的{local_total}次请求，因此不能用它评价用户侧综合时延；用户侧应查看独立的业务GET超时率{biz_timeout_rate:.4f}%。

六、为什么Redis没有减压
----------------------
基线阶段3业务GET为{base_get}次，即{base_get_qps:.1f}/s，全部访问Redis。优化1阶段3业务GET扩大到{biz_total}次，即{biz_qps:.1f}/s；其中20%回源后仍为{origin_total}次，即{origin_qps:.1f}/s，与基线基本相同。

当前客户端属于闭环满速压测：一次请求完成后马上发送下一次。本地缓存让80%的GET几乎立即完成，因此同一线程在60秒内执行了更多业务请求。总业务量扩大约{biz_qps/base_get_qps:.2f}倍后，20%的回源正好抵消了80%的缓存比例。

因此这轮实验验证了容量提升：在Redis压力基本不变时，系统能处理约5倍业务GET。若要验证Redis降载，需要把基线和优化1的业务GET总QPS固定为相同值。例如固定为{base_get_qps:.0f}/s，80%本地返回后，Redis回源应约为{base_get_qps*0.2:.0f}/s。

七、内存、复制、丢包和大Key
--------------------------
阶段3内存增长{sm['mem3_growth']:.0f} bytes，与基线相比变化{pct(bsm['mem3_growth'],sm['mem3_growth']):+.2f}%。最大主从offset差为{sm['repl3_gap']:.0f} bytes，较基线增加{pct(bsm['repl3_gap'],sm['repl3_gap']):+.2f}%，但复制链路异常采样为0，结束时主从offset一致。RX/TX丢包及错误增量均为0。

本轮仍检测到最大2MB String，这是预期现象：优化1只验证客户端缓存，没有启用优化2的大Key保护。

八、最终结论
------------
1. 优化1代码和缓存比例工作正常，实际本地服务比例为{actual:.4f}%。
2. 业务GET综合平均RTT下降{-pct(brmap[("阶段3","GET")][0],biz_lat[0]):.2f}%，业务吞吐提高到基线的{biz_qps/base_get_qps:.2f}倍。
3. 业务GET仍有{biz_timeout_rate:.4f}%超过5ms，P95为{biz_lat[2]/1000:.2f}ms，严格5ms目标尚未完全满足。
4. Redis回源量、CPU和网卡流量没有下降，本轮不能作为“Redis减压80%”的证据。
5. 当前结果适合表述为：客户端缓存显著提高业务容量并降低用户平均时延，但在满速闭环压测下没有降低Redis绝对负载。
6. 为评价相同业务量下的降载效果，下一轮应增加固定业务GET QPS或固定请求总数的测试模式，并至少重复3轮取中位数。
"""
(OUT/"完整分析报告.txt").write_text(report,encoding="utf-8")

cmp_text=f"""优化1与新基线对比结论
========================

对比轮次：新基线{BASE_RUN}；优化1 {RUN}。

1. 缓存实际本地服务比例{actual:.4f}%，功能正确生效。
2. 业务GET综合平均RTT由{brmap[("阶段3","GET")][0]}us降到{biz_lat[0]}us，下降{-pct(brmap[("阶段3","GET")][0],biz_lat[0]):.2f}%。
3. 业务GET吞吐由{base_get_qps:.1f}/s提升到{biz_qps:.1f}/s，达到基线的{biz_qps/base_get_qps:.2f}倍。
4. 业务GET超5ms比例为{biz_timeout_rate:.4f}%，P95={biz_lat[2]/1000:.2f}ms，尚未完全达到5ms目标。
5. Redis回源GET由{base_get}次变为{origin_total}次，变化{pct(base_get,origin_total):+.2f}%；TX峰值变化{pct(bsm['tx3_peak'],sm['tx3_peak']):+.2f}%，CPU稳态平均增加{o_cpu3_steady_avg-b_cpu3_steady_avg:.2f}个百分点，基本相同。
6. 主从最终同步、无复制链路异常、无网卡丢包，但最大复制offset差较基线增大。

结论：这轮证明了“保持Redis接近同等压力时，客户端缓存让业务GET容量提高约5倍并把平均时延降低约80%”。由于满速压测自动放大业务量，它没有证明相同业务量下Redis网卡和CPU下降。下一轮如需验证Redis减压，应固定业务GET总QPS。
"""
(CMP/"对比结论.txt").write_text(cmp_text,encoding="utf-8")

print(f"single={OUT}")
print(f"compare={CMP}")

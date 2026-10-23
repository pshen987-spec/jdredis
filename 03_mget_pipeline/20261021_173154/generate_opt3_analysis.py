#!/usr/bin/env python3
import csv
import re
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parent
BASE = ROOT.parents[1] / "00_baseline" / "20261021_160056"
RUN = "20261021_173154"
OUT = ROOT / f"opt3_pipeline_{RUN}数据"
CMP = ROOT / "comparison_baseline_vs_opt3"
OUT.mkdir(exist_ok=True)
CMP.mkdir(exist_ok=True)

FONT = Path(r"C:\Windows\Fonts\msyh.ttc")
FONT_B = Path(r"C:\Windows\Fonts\msyhbd.ttc")
COLORS = {"blue": "#2563eb", "orange": "#ea580c", "green": "#16a34a", "red": "#dc2626",
          "grid": "#dbe3ee", "text": "#172033", "muted": "#667085", "bg": "#ffffff"}

def font(size, bold=False):
    return ImageFont.truetype(str(FONT_B if bold else FONT), size)

def hms_seconds(s):
    h, m, sec = map(int, s.split(":")); return h * 3600 + m * 60 + sec

def parse_cpu(run_dir, run_id):
    p = run_dir / "logs" / f"top_h_{run_id}.log"
    rows, cur, seen_phase3 = [], None, False
    for raw in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        m = re.match(r"^==== (?:pidstat|top)-snap (\d\d:\d\d:\d\d) phase=([^ ]*) ====", raw)
        if m:
            if cur: rows.append(cur)
            raw_phase = m.group(2)
            if raw_phase == "3": seen_phase3 = True
            ph = {"2": "阶段2", "3": "阶段3"}.get(raw_phase, "结束" if seen_phase3 else "阶段1/预热")
            cur = [m.group(1), ph, 0.0]
            continue
        if not cur or "Average:" in raw or "redis-server" not in raw:
            continue
        parts = raw.split()
        try:
            value = float(parts[-3])
            cur[2] = max(cur[2], value)
        except (ValueError, IndexError):
            pass
    if cur: rows.append(cur)
    return rows

def phase_times(run_dir, run_id):
    text = (run_dir / "logs" / f"top_h_{run_id}.log").read_text(encoding="utf-8", errors="ignore")
    p2 = re.search(r"^==== .*? (\d\d:\d\d:\d\d) phase=2 ====", text, re.M)
    p3 = re.search(r"^==== .*? (\d\d:\d\d:\d\d) phase=3 ====", text, re.M)
    return hms_seconds(p2.group(1)), hms_seconds(p3.group(1))

def parse_net(run_dir, run_id, iface="enp165s0f0np0"):
    p2, p3 = phase_times(run_dir, run_id)
    p = run_dir / "logs" / f"net_dev_{run_id}.log"
    snaps, tm, counters = [], None, None
    for raw in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        m = re.match(r"^==== (\d\d:\d\d:\d\d) ====", raw)
        if m:
            tm, counters = m.group(1), None
            continue
        parts = raw.split()
        if tm and len(parts) == 3 and parts[0] == iface:
            counters = (int(parts[1]), int(parts[2]))
            snaps.append((tm, *counters))
    rows = []
    for prev, cur in zip(snaps, snaps[1:]):
        sec = hms_seconds(cur[0])
        # 阶段3从首个phase=3时间点到60秒结束点（含首尾，共61个1秒差分样本）。
        ph = "结束" if sec > p3 + 60 else ("阶段3" if sec >= p3 else ("阶段2" if sec >= p2 else "阶段1/预热"))
        rows.append([cur[0], ph, (cur[1]-prev[1])/1048576, (cur[2]-prev[2])/1048576])
    return rows

def parse_rtt(run_dir, run_id):
    p = run_dir / "logs" / f"client_out_{run_id}.log"
    rows = []
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        m = re.match(r"\[(阶段2|阶段3|全程) RTT\(us\)\] (.*)", line)
        if not m: continue
        for cmd, av, mx in re.findall(r"(MGET|SET|GET|APPEND) avg=(-?\d+) max=(-?\d+)", m.group(2)):
            rows.append([m.group(1), cmd, int(av), int(mx)])
    return rows

def write_csv(path, header, rows):
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f); w.writerow(header); w.writerows(rows)

def canvas(title, subtitle="", w=1500, h=820):
    im = Image.new("RGB", (w, h), COLORS["bg"]); d = ImageDraw.Draw(im)
    d.text((70, 34), title, font=font(34, True), fill=COLORS["text"])
    if subtitle: d.text((70, 80), subtitle, font=font(19), fill=COLORS["muted"])
    return im, d

def line_chart(path, title, subtitle, series, y_label, y_min=0, y_max=None):
    im, d = canvas(title, subtitle); w, h = im.size
    L, R, T, B = 115, 55, 135, 110; pw, ph = w-L-R, h-T-B
    vals = [v for _, ys, _ in series for v in ys if v is not None]
    ymax = y_max if y_max is not None else max(vals or [1]) * 1.12
    ymin = y_min
    for i in range(6):
        y = T + ph * i / 5; val = ymax-(ymax-ymin)*i/5
        d.line((L, y, w-R, y), fill=COLORS["grid"], width=1)
        d.text((18, y-12), f"{val:,.0f}", font=font(16), fill=COLORS["muted"])
    d.line((L,T,L,T+ph),fill=COLORS["text"],width=2); d.line((L,T+ph,w-R,T+ph),fill=COLORS["text"],width=2)
    maxn = max(len(ys) for _,ys,_ in series)
    for name, ys, color in series:
        pts=[]
        for i,v in enumerate(ys):
            x=L+(i/(maxn-1 if maxn>1 else 1))*pw
            y=T+ph*(ymax-v)/(ymax-ymin if ymax>ymin else 1)
            pts.append((x,y))
        if len(pts)>1: d.line(pts,fill=color,width=4,joint="curve")
    d.text((L, h-65), "样本顺序 / 阶段内经过时间", font=font(17), fill=COLORS["muted"])
    d.text((15, 105), y_label, font=font(17), fill=COLORS["muted"])
    x=L
    for name,_,color in series:
        d.line((x,h-35,x+38,h-35),fill=color,width=5); d.text((x+48,h-49),name,font=font(18),fill=COLORS["text"]); x+=260
    im.save(path,"JPEG",quality=94,subsampling=0)

def phase_line_chart(path, title, subtitle, rows, columns, colors, y_label, ymax=None):
    im,d=canvas(title,subtitle); w,h=im.size; L,R,T,B=115,55,135,120; pw,ph=w-L-R,h-T-B
    vals=[float(r[c]) for r in rows for c in columns]
    ym=(ymax if ymax else max(vals)*1.12) or 1
    for i in range(6):
        y=T+ph*i/5; v=ym*(5-i)/5; d.line((L,y,w-R,y),fill=COLORS["grid"],width=1); d.text((20,y-12),f"{v:,.0f}",font=font(16),fill=COLORS["muted"])
    phases=[r[1] for r in rows]
    for i in range(1,len(rows)):
        if phases[i]!=phases[i-1]:
            x=L+i/(len(rows)-1)*pw; d.line((x,T,x,T+ph),fill="#94a3b8",width=2)
            d.text((x+8,T+8),phases[i],font=font(16,True),fill=COLORS["muted"])
    d.line((L,T,L,T+ph),fill=COLORS["text"],width=2); d.line((L,T+ph,w-R,T+ph),fill=COLORS["text"],width=2)
    for c,color,label in zip(columns,colors,["CPU" if len(columns)==1 else "RX","TX"]):
        pts=[]
        for i,r in enumerate(rows):
            x=L+i/(len(rows)-1)*pw; y=T+ph*(ym-float(r[c]))/ym; pts.append((x,y))
        d.line(pts,fill=color,width=4,joint="curve")
    d.text((18,105),y_label,font=font(17),fill=COLORS["muted"])
    legend_labels=["CPU"] if len(columns)==1 else ["RX","TX"]
    x=L
    for label,color in zip(legend_labels,colors):
        d.line((x,h-38,x+38,h-38),fill=color,width=5); d.text((x+48,h-52),label,font=font(18),fill=COLORS["text"]); x+=170
    im.save(path,"JPEG",quality=94,subsampling=0)

def category_chart(path,title,subtitle,categories,series,y_label):
    im,d=canvas(title,subtitle); w,h=im.size; L,R,T,B=125,55,145,135; pw,ph=w-L-R,h-T-B
    vals=[v for _,vs,_ in series for v in vs if v>=0]; ym=max(vals or [1])*1.18
    for i in range(6):
        y=T+ph*i/5; v=ym*(5-i)/5; d.line((L,y,w-R,y),fill=COLORS["grid"],width=1); d.text((20,y-12),f"{v:,.0f}",font=font(16),fill=COLORS["muted"])
    for si,(name,vs,color) in enumerate(series):
        pts=[]
        for i,v in enumerate(vs):
            x=L+(i+0.5)/len(categories)*pw
            if v<0:
                d.text((x-27,T+ph-30-si*28),"未执行",font=font(14),fill=COLORS["muted"]); continue
            y=T+ph*(ym-v)/ym; pts.append((x,y)); d.ellipse((x-5,y-5,x+5,y+5),fill=color)
            d.text((x-35,y-30-si*24),f"{v:,.0f}",font=font(14),fill=color)
        if len(pts)>1: d.line(pts,fill=color,width=4)
    for i,c in enumerate(categories):
        x=L+(i+0.5)/len(categories)*pw; d.text((x-45,T+ph+22),c,font=font(17,True),fill=COLORS["text"])
    d.text((20,112),y_label,font=font(17),fill=COLORS["muted"])
    x=L
    for name,_,color in series:
        d.line((x,h-40,x+38,h-40),fill=color,width=5); d.text((x+48,h-54),name,font=font(17),fill=COLORS["text"]); x+=320
    im.save(path,"JPEG",quality=94,subsampling=0)

cpu=parse_cpu(ROOT,RUN); net=parse_net(ROOT,RUN); rtt=parse_rtt(ROOT,RUN)
bcpu=parse_cpu(BASE,"20261021_160056"); bnet=parse_net(BASE,"20261021_160056"); brtt=parse_rtt(BASE,"20261021_160056")

write_csv(OUT/f"CPU_opt3_pipeline_{RUN}.csv",["time","phase","cpu_max_pct"],[[a,b,f"{c:.2f}"] for a,b,c in cpu])
write_csv(OUT/f"NET_opt3_pipeline_{RUN}.csv",["time","phase","rx_mib_s","tx_mib_s"],[[a,b,f"{c:.2f}",f"{d:.2f}"] for a,b,c,d in net])
write_csv(OUT/f"RTT_opt3_pipeline_{RUN}.csv",["phase","cmd","avg_us","max_us"],rtt)

phase_line_chart(OUT/f"CPU_opt3_pipeline_{RUN}_chart.jpg","优化3 CPU 全阶段折线图","MGET 800 Key拆为80×MGET(10 Key)，Y轴固定0–100%",cpu,[2],[COLORS["blue"]],"CPU (%)",100)
phase_line_chart(OUT/f"NET_opt3_pipeline_{RUN}_chart.jpg","优化3网络流量全阶段折线图","相邻网卡累计计数差，单位MiB/s",net,[2,3],[COLORS["blue"],COLORS["orange"]],"MiB/s")
rtt3={cmd:(av,mx) for ph,cmd,av,mx in rtt if ph=="阶段3"}
category_chart(OUT/f"RTT_opt3_pipeline_{RUN}_chart.jpg","优化3阶段3 RTT","MGET为拆分后的10 Key子命令；完整800 Key请求另见对比图",["MGET(10)","SET","GET","APPEND"],[("平均RTT",[rtt3[x][0] for x in ["MGET","SET","GET","APPEND"]],COLORS["blue"]),("最大RTT",[rtt3[x][1] for x in ["MGET","SET","GET","APPEND"]],COLORS["orange"])],"RTT (us)")

op3=[r for r in cpu if r[1]=="阶段3"]; bp3=[r for r in bcpu if r[1]=="阶段3"]
line_chart(CMP/"01_CPU双折线对比.jpg","阶段3 CPU双折线对比","两轮均接近Redis单核满载，Y轴固定0–100%",[("基线",[r[2] for r in bp3],COLORS["blue"]),("优化3",[r[2] for r in op3],COLORS["orange"])],"CPU (%)",0,100)
on3=[r for r in net if r[1]=="阶段3"]; bn3=[r for r in bnet if r[1]=="阶段3"]
line_chart(CMP/"02_阶段3_TX双折线对比.jpg","阶段3 TX双折线对比","优化3平均TX下降，但完整800 Key请求吞吐也下降",[("基线",[r[3] for r in bn3],COLORS["blue"]),("优化3",[r[3] for r in on3],COLORS["orange"])],"TX (MiB/s)")
br3={cmd:av for ph,cmd,av,mx in brtt if ph=="阶段3"}
category_chart(CMP/"03_阶段3平均RTT双折线对比.jpg","阶段3平均RTT对比","注意：基线MGET为800 Key，优化3 MGET为10 Key子命令，MGET不属同口径",["MGET*","SET","GET","APPEND"],[("基线",[br3[x] for x in ["MGET","SET","GET","APPEND"]],COLORS["blue"]),("优化3",[rtt3[x][0] for x in ["MGET","SET","GET","APPEND"]],COLORS["orange"])],"平均RTT (us)")
category_chart(CMP/"04_完整800Key逻辑请求对比.jpg","完整800 Key逻辑请求对比","吞吐越高越好；平均周期和超时次数越低越好",["吞吐(次/s)","估算周期(ms)",">100ms次数"],[("基线",[654.1,30.57,6],COLORS["blue"]),("优化3",[319.2,62.67,75],COLORS["orange"])],"数值（各指标单位见横轴）")

comparison=[
 ["阶段3 CPU平均","%","97.05","96.68","-0.4%","基本持平，仍接近单核满载"],
 ["阶段3 TX平均","MiB/s","2423.77","2091.17","-13.7%","发送流量下降，但伴随逻辑吞吐下降"],
 ["阶段3 TX峰值","kB/s","2794097","2363736","-15.4%","发送峰值下降"],
 ["阶段3 RX平均","MiB/s","458.26","577.89","+26.1%","接收流量上升"],
 ["阶段3 SET平均RTT","us","8125","5838","-28.1%","改善"],
 ["阶段3 GET平均RTT","us","8077","6152","-23.8%","改善"],
 ["阶段3 APPEND平均RTT","us","6453","5440","-15.7%","改善"],
 ["阶段3 MGET子命令平均RTT","us","30476（800 Key）","5956（10 Key）","不同口径","仅说明单条子命令缩短，不能直接作为业务改善比例"],
 ["完整800 Key请求完成量","次","39248","19149","-51.2%","阶段3逻辑完成量"],
 ["完整800 Key请求吞吐","次/s","654.1","319.2","-51.2%","业务吞吐下降"],
 ["完整800 Key请求估算周期","ms","30.57","62.67","+105.0%","按20个持续线程估算"],
 ["完整请求RTT>100ms","次","6","75","+1150.0%","超时次数增加"],
 ["完整请求超时比例","%","0.0153","0.3917","约25.6倍","优化3更差"],
 ["阶段3内存增长","bytes","3944214840","3612981256","-8.3%","略降"],
 ["阶段3最大复制offset差","bytes","614650","3892792","+533.3%","过程积压增大，最终已追平"],
]
write_csv(CMP/"关键数据对比.csv",["指标","单位","最新基线","优化3 MGET拆分Pipeline","变化百分比","说明"],comparison)
write_csv(CMP/"完整800Key逻辑请求对比.csv",["指标","单位","最新基线","优化3","变化"],[["阶段2完成量","次",32016,18170,"-43.2%"],["阶段3完成量","次",39248,19149,"-51.2%"],["阶段3吞吐","次/s",654.1,319.2,"-51.2%"],["阶段3估算周期","ms",30.57,62.67,"+105.0%"],["阶段3 RTT>100ms","次",6,75,"+1150.0%"],["阶段3超时比例","%",0.0153,0.3917,"约25.6倍"]])

(OUT/"本轮数据摘要.txt").write_text("""优化3：MGET拆分 + Pipeline，本轮20261021_173154。\n阶段3CPU平均96.6775%，峰值98%。阶段3TX平均2091.17MiB/s、峰值2363736kB/s；RX平均577.89MiB/s。\n阶段3子命令RTT(us)：MGET(10 Key) 5956/37735，SET 5838/30982，GET 6152/20515，APPEND 5440/18880（平均/最大）。\n拆分后的子命令RTT>100ms为0，但完整800 Key逻辑请求阶段3完成19149次，其中75次超过100ms。\n主从最终offset一致，无复制异常采样和网卡丢包。\n""",encoding="utf-8")

analysis=(ROOT/"优化3_结果分析.txt").read_text(encoding="utf-8")
(OUT/"完整分析报告.txt").write_text(analysis,encoding="utf-8")
(CMP/"对比结论.txt").write_text("""优化3：MGET拆分 + Pipeline——与最新基线对比结论\n================================================\n\n对比轮次：基线20261021_160056；优化3 20261021_173154。\n\n1. 单条MGET从800 Key拆成10 Key后，阶段3子命令平均RTT为5956us，最大37735us，单条子命令没有超过100ms。\n2. SET、GET、APPEND平均RTT分别下降28.1%、23.8%和15.7%，说明其他命令获得了更多调度机会。\n3. 阶段3TX平均下降13.7%、TX峰值下降15.4%，但阶段3RX平均上升26.1%。\n4. 完整800 Key逻辑请求吞吐从654.1次/秒降到319.2次/秒，下降51.2%；估算平均周期从30.57ms增至62.67ms。\n5. 完整逻辑请求超过100ms由6次增至75次，超时比例约为基线的25.6倍。\n6. CPU平均仅下降0.4%，仍接近单核满载；最大复制offset差增大533.3%，但最终主从追平且无丢包。\n7. 结论：命令公平性和其他命令平均RTT有所改善，但当前10 Key批次、Pipeline深度8使完整业务请求明显变慢，本轮部分有效，暂不通过最终验收。\n""",encoding="utf-8")

print(OUT)
print(CMP)

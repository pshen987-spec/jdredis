#!/usr/bin/env python3
import csv
import re
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parent
BASE = ROOT.parents[1] / "00_baseline" / "20261021_160056"
RUN = "20261022_152323"
OUT = ROOT / f"opt5_soft_ttl_{RUN}数据"
CMP = ROOT / "comparison_baseline_vs_opt5"
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

# 客户端已在15:26:28前停止，但采样先读取旧phase=3再测得空闲CPU，修正为结束阶段。
for row in cpu:
    if row[1] == "阶段3" and row[0] == "15:26:28" and row[2] < 10:
        row[1] = "结束"
for row in net:
    if row[1] == "阶段3" and row[0] >= "15:26:28":
        row[1] = "结束"

write_csv(OUT/f"CPU_opt5_soft_ttl_{RUN}.csv",["time","phase","cpu_max_pct"],[[a,b,f"{c:.2f}"] for a,b,c in cpu])
write_csv(OUT/f"NET_opt5_soft_ttl_{RUN}.csv",["time","phase","rx_mib_s","tx_mib_s"],[[a,b,f"{c:.2f}",f"{d:.2f}"] for a,b,c,d in net])
write_csv(OUT/f"RTT_opt5_soft_ttl_{RUN}.csv",["phase","cmd","avg_us","max_us"],rtt)

phase_line_chart(OUT/f"CPU_opt5_soft_ttl_{RUN}_chart.jpg","优化5 CPU 全阶段折线图","逻辑TTL 1秒、Redis物理TTL 10秒；Y轴固定0–100%",cpu,[2],[COLORS["blue"]],"CPU (%)",100)
phase_line_chart(OUT/f"NET_opt5_soft_ttl_{RUN}_chart.jpg","优化5网络流量全阶段折线图","相邻网卡累计计数差，单位MiB/s",net,[2,3],[COLORS["blue"],COLORS["orange"]],"MiB/s")
rtt3={cmd:(av,mx) for ph,cmd,av,mx in rtt if ph=="阶段3"}
category_chart(OUT/f"RTT_opt5_soft_ttl_{RUN}_chart.jpg","优化5阶段3 RTT","平均/最大为单条命令端到端RTT；本轮仍是旧版100ms超时口径",["MGET","SET","GET","APPEND"],[("平均RTT",[rtt3[x][0] for x in ["MGET","SET","GET","APPEND"]],COLORS["blue"]),("最大RTT",[rtt3[x][1] for x in ["MGET","SET","GET","APPEND"]],COLORS["orange"])],"RTT (us)")

op5=[r for r in cpu if r[1]=="阶段3"]; bp3=[r for r in bcpu if r[1]=="阶段3"]
line_chart(CMP/"01_CPU双折线对比.jpg","阶段3 CPU双折线对比","剔除优化5最后一个误标为阶段3的空闲快照；两轮仍接近单核满载",[("基线",[r[2] for r in bp3],COLORS["blue"]),("优化5",[r[2] for r in op5],COLORS["orange"])],"CPU (%)",0,100)
on3=[r for r in net if r[1]=="阶段3"]; bn3=[r for r in bnet if r[1]=="阶段3"]
line_chart(CMP/"02_阶段3_TX双折线对比.jpg","阶段3 TX双折线对比","优化5命令完成量同步下降，流量变化不能单独视为优化收益",[("基线",[r[3] for r in bn3],COLORS["blue"]),("优化5",[r[3] for r in on3],COLORS["orange"])],"TX (MiB/s)")
br3={cmd:av for ph,cmd,av,mx in brtt if ph=="阶段3"}
category_chart(CMP/"03_阶段3平均RTT双折线对比.jpg","阶段3平均RTT对比","四类命令同口径；优化5平均RTT均略有上升",["MGET","SET","GET","APPEND"],[("基线",[br3[x] for x in ["MGET","SET","GET","APPEND"]],COLORS["blue"]),("优化5",[rtt3[x][0] for x in ["MGET","SET","GET","APPEND"]],COLORS["orange"])],"平均RTT (us)")
category_chart(CMP/"04_阶段3命令完成量对比.jpg","阶段3命令完成量对比","优化5四类命令完成量均低于基线",["MGET","SET","GET","APPEND"],[("基线",[39248,117950,296682,11335],COLORS["blue"]),("优化5",[38504,113771,291213,11240],COLORS["orange"])],"完成次数")

comparison=[
 ["阶段3 CPU平均（原摘要）","%","97.05","92.53","-4.7%","含一个压测结束后的3%/0%空闲误标样本"],
 ["阶段3 CPU平均（修正）","%","97.05","97.32","+0.3%","剔除空闲误标样本，基本持平"],
 ["阶段3 TX峰值","kB/s","2794097","2723169","-2.5%","命令完成量同时下降"],
 ["阶段3 RX峰值","kB/s","573440","545226","-4.9%","命令完成量同时下降"],
 ["阶段3 MGET完成量","次","39248","38504","-1.9%","下降"],
 ["阶段3 SET完成量","次","117950","113771","-3.5%","下降"],
 ["阶段3 GET完成量","次","296682","291213","-1.8%","下降"],
 ["阶段3 APPEND完成量","次","11335","11240","-0.8%","下降"],
 ["阶段3 MGET平均RTT","us","30476","31079","+2.0%","变慢"],
 ["阶段3 SET平均RTT","us","8125","8511","+4.8%","变慢"],
 ["阶段3 GET平均RTT","us","8077","8228","+1.9%","变慢"],
 ["阶段3 APPEND平均RTT","us","6453","6666","+3.3%","变慢"],
 ["全程RTT>100ms","次","6","11","+83.3%","旧版100ms口径"],
 ["阶段3内存增长","bytes","3944214840","3416141896","-13.4%","下降，但负载也略降"],
 ["阶段3最大复制offset差","bytes","614650","1229303","+100.0%","过程积压增大，最终已追平"],
]
write_csv(CMP/"关键数据对比.csv",["指标","单位","最新基线","优化5逻辑TTL","变化百分比","说明"],comparison)
write_csv(CMP/"逻辑TTL统计.csv",["指标","数值","单位","说明"],[["逻辑TTL刷新",58,"次","约每秒一次"],["GET返回过期旧值",17847,"次","逻辑过期期间仍返回旧数据"],["阶段3 GET总数",291213,"次","客户端完成量"],["旧值返回比例",6.13,"%","17847/291213"],["未获得刷新锁",0,"次","仅一个刷新线程，未形成锁竞争"]])

(OUT/"本轮数据摘要.txt").write_text("""优化5：TTL逻辑过期，本轮20261022_152323。\n逻辑TTL刷新58次，17847次GET返回过期旧值，占阶段3 GET的6.13%。\n阶段3CPU摘要平均92.5265%，但含最后一个误标的空闲快照；剔除后约97.3174%，与基线基本持平。\n阶段3RTT(us)：MGET 31079/131686，SET 8511/38403，GET 8228/22501，APPEND 6666/18987（平均/最大）。\n全程RTT>100ms为11次；本轮没有采集新规则下GET>5ms次数。\n阶段3内存增长3416141896 bytes；主从最终offset一致，无复制异常采样和网卡丢包。\n""",encoding="utf-8")

analysis=(ROOT/"优化5_结果分析.txt").read_text(encoding="utf-8")
(OUT/"完整分析报告.txt").write_text(analysis,encoding="utf-8")
(CMP/"对比结论.txt").write_text("""优化5：TTL逻辑过期——与最新基线对比结论
============================================

对比轮次：基线20261021_160056；优化5 20261022_152323。

1. 逻辑TTL刷新58次，17847次GET返回逻辑过期旧值，占阶段3GET的6.13%，功能确实生效。
2. 原摘要CPU平均92.53%包含一个压测结束后的空闲误标样本；剔除后约97.32%，与基线97.05%基本持平。
3. MGET、SET、GET、APPEND阶段3完成量分别下降1.9%、3.5%、1.8%和0.8%。
4. 四类命令平均RTT分别上升2.0%、4.8%、1.9%和3.3%；100ms以上命令由6次增至11次。
5. 阶段3内存增长下降13.4%，但负载也略降，且逻辑TTL只覆盖一个热Key，不能把全部差异归因于TTL优化。
6. 主从最终offset一致、无复制异常和网卡丢包；最大复制offset差约翻倍。
7. 本轮仍是旧版100ms口径，没有GET>5ms计数。结论为功能验证通过，性能验收暂不通过。
""",encoding="utf-8")

print(OUT)
print(CMP)

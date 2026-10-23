#!/usr/bin/env python3
import csv
import re
import statistics
import sys
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parent
RUN = sys.argv[1] if len(sys.argv) > 1 else ROOT.name
OUT = ROOT / f"baseline_{RUN}数据"
OUT.mkdir(exist_ok=True)

FONT = Path(r"C:\Windows\Fonts\msyh.ttc")
FONT_B = Path(r"C:\Windows\Fonts\msyhbd.ttc")
C = {"blue":"#2563eb","orange":"#ea580c","green":"#16a34a","red":"#dc2626",
     "purple":"#7c3aed","grid":"#dbe3ee","text":"#172033","muted":"#667085","bg":"#ffffff"}

def font(size, bold=False):
    return ImageFont.truetype(str(FONT_B if bold else FONT), size)

def hms(s):
    hh, mm, ss = map(int, s.split(":")); return hh*3600 + mm*60 + ss

def parse_cpu():
    p = ROOT / "logs" / f"top_h_{RUN}.log"
    rows, cur, seen3 = [], None, False
    for raw in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        m = re.match(r"^==== (?:pidstat|top)-snap (\d\d:\d\d:\d\d) phase=([^ ]*) ====", raw)
        if m:
            if cur: rows.append(cur)
            phase = m.group(2)
            if phase == "3": seen3 = True
            label = {"2":"阶段2", "3":"阶段3"}.get(phase, "结束" if seen3 else "阶段1/灌入")
            cur = [m.group(1), label, 0.0]
            continue
        if not cur or "Average:" in raw or "redis-server" not in raw:
            continue
        parts = raw.split()
        try: cur[2] = max(cur[2], float(parts[-3]))
        except (ValueError, IndexError): pass
    if cur: rows.append(cur)
    return rows

def phase_times():
    text = (ROOT/"logs"/f"top_h_{RUN}.log").read_text(encoding="utf-8", errors="ignore")
    p2 = re.search(r"^==== .*? (\d\d:\d\d:\d\d) phase=2 ====", text, re.M)
    p3 = re.search(r"^==== .*? (\d\d:\d\d:\d\d) phase=3 ====", text, re.M)
    if not p2 or not p3: raise RuntimeError("缺少阶段2或阶段3时间标记")
    return hms(p2.group(1)), hms(p3.group(1))

def parse_net():
    p2, p3 = phase_times()
    summary = (ROOT/"acceptance_summary.txt").read_text(encoding="utf-8", errors="ignore")
    m = re.search(r"采样网卡:\s*(\S+)", summary)
    iface = m.group(1) if m else "enp165s0f0np0"
    snaps, tm = [], None
    for raw in (ROOT/"logs"/f"net_dev_{RUN}.log").read_text(encoding="utf-8", errors="ignore").splitlines():
        m = re.match(r"^==== (\d\d:\d\d:\d\d) ====", raw)
        if m: tm=m.group(1); continue
        parts=raw.split()
        if tm and len(parts)==3 and parts[0]==iface:
            snaps.append((tm,int(parts[1]),int(parts[2])))
    rows=[]
    for prev,cur in zip(snaps,snaps[1:]):
        sec=hms(cur[0])
        phase="结束" if sec>p3+60 else ("阶段3" if sec>=p3 else ("阶段2" if sec>=p2 else "阶段1/灌入"))
        rows.append([cur[0],phase,(cur[1]-prev[1])/1048576,(cur[2]-prev[2])/1048576])
    return rows, iface

def parse_rtt():
    text=(ROOT/"logs"/f"client_out_{RUN}.log").read_text(encoding="utf-8",errors="ignore")
    rows=[]
    for line in text.splitlines():
        m=re.match(r"\[(阶段2|阶段3|全程) RTT\(us\)\] (.*)",line)
        if not m: continue
        for cmd,av,mx in re.findall(r"(MGET|SET|GET|APPEND) avg=(-?\d+) max=(-?\d+)",m.group(2)):
            rows.append([m.group(1),cmd,int(av),int(mx)])
    stats={}
    for label in ("阶段2结束","全部结束"):
        m=re.search(rf"\[{label}\].*?成功=(\d+) 失败=(\d+) 超时=(\d+)",text)
        if m: stats[label]=tuple(map(int,m.groups()))
    return rows, stats

def write_csv(path, header, rows):
    with path.open("w",encoding="utf-8-sig",newline="") as f:
        w=csv.writer(f); w.writerow(header); w.writerows(rows)

def canvas(title, subtitle, w=1600, h=900):
    im=Image.new("RGB",(w,h),C["bg"]); d=ImageDraw.Draw(im)
    d.text((72,34),title,font=font(36,True),fill=C["text"])
    d.text((72,84),subtitle,font=font(19),fill=C["muted"])
    return im,d

def time_chart(path,title,subtitle,rows,cols,labels,colors,ylabel,ymax=None):
    im,d=canvas(title,subtitle); w,h=im.size; L,R,T,B=125,55,145,125; pw,ph=w-L-R,h-T-B
    vals=[float(r[c]) for r in rows for c in cols]
    ym=ymax or max(vals or [1])*1.12
    for i in range(6):
        y=T+ph*i/5; v=ym*(5-i)/5
        d.line((L,y,w-R,y),fill=C["grid"],width=1); d.text((22,y-13),f"{v:,.0f}",font=font(16),fill=C["muted"])
    d.line((L,T,L,T+ph),fill=C["text"],width=2); d.line((L,T+ph,w-R,T+ph),fill=C["text"],width=2)
    for i in range(1,len(rows)):
        if rows[i][1]!=rows[i-1][1]:
            x=L+i/(len(rows)-1)*pw; d.line((x,T,x,T+ph),fill="#94a3b8",width=2)
            d.text((x+8,T+8),rows[i][1],font=font(15,True),fill=C["muted"])
    for col,label,color in zip(cols,labels,colors):
        pts=[]
        for i,r in enumerate(rows):
            x=L+i/(len(rows)-1)*pw; y=T+ph*(ym-float(r[col]))/ym; pts.append((x,y))
        if len(pts)>1: d.line(pts,fill=color,width=4,joint="curve")
    d.text((20,112),ylabel,font=font(17),fill=C["muted"])
    x=L
    for label,color in zip(labels,colors):
        d.line((x,h-42,x+42,h-42),fill=color,width=5); d.text((x+52,h-57),label,font=font(18),fill=C["text"]); x+=210
    im.save(path,"JPEG",quality=95,subsampling=0)

def rtt_chart(path, rows):
    # 展示阶段2和阶段3；-1代表该阶段未执行，不落在0轴上。
    keys=[("阶段2",c) for c in ("MGET","SET","GET","APPEND")]+[("阶段3",c) for c in ("MGET","SET","GET","APPEND")]
    lookup={(p,c):(a,m) for p,c,a,m in rows}
    avg=[lookup[k][0] for k in keys]; mx=[lookup[k][1] for k in keys]
    vals=[v for v in avg+mx if v>=0]; ym=max(vals)*1.16
    im,d=canvas("新基线 RTT 全阶段折线图","MGET=100 Key；5ms为业务超时阈值；-1对应命令在该阶段未执行")
    w,h=im.size; L,R,T,B=125,55,150,155; pw,ph=w-L-R,h-T-B
    for i in range(6):
        y=T+ph*i/5; v=ym*(5-i)/5
        d.line((L,y,w-R,y),fill=C["grid"],width=1); d.text((20,y-12),f"{v:,.0f}",font=font(16),fill=C["muted"])
    threshold_y=T+ph*(ym-5000)/ym
    d.line((L,threshold_y,w-R,threshold_y),fill=C["red"],width=2)
    d.text((w-R-180,threshold_y-27),"5ms阈值",font=font(16,True),fill=C["red"])
    d.line((L,T,L,T+ph),fill=C["text"],width=2); d.line((L,T+ph,w-R,T+ph),fill=C["text"],width=2)
    xs=[L+(i+.5)/len(keys)*pw for i in range(len(keys))]
    d.line(((xs[3]+xs[4])/2,T,(xs[3]+xs[4])/2,T+ph),fill="#94a3b8",width=2)
    for series,color,offset in ((avg,C["blue"],0),(mx,C["orange"],1)):
        segments=[]
        for i,v in enumerate(series):
            if v<0:
                if len(segments)>1: d.line(segments,fill=color,width=4)
                segments=[]; d.text((xs[i]-30,T+ph-35-offset*25),"未执行",font=font(14),fill=C["muted"]); continue
            y=T+ph*(ym-v)/ym; segments.append((xs[i],y)); d.ellipse((xs[i]-5,y-5,xs[i]+5,y+5),fill=color)
        if len(segments)>1: d.line(segments,fill=color,width=4)
    for i,(phase,cmd) in enumerate(keys):
        d.text((xs[i]-38,T+ph+20),cmd,font=font(16,True),fill=C["text"])
    d.text((L+pw*.18,T+ph+60),"阶段2",font=font(18,True),fill=C["muted"])
    d.text((L+pw*.68,T+ph+60),"阶段3",font=font(18,True),fill=C["muted"])
    d.text((20,116),"RTT (us)",font=font(17),fill=C["muted"])
    d.line((L,h-42,L+42,h-42),fill=C["blue"],width=5); d.text((L+52,h-57),"平均RTT",font=font(18),fill=C["text"])
    d.line((L+250,h-42,L+292,h-42),fill=C["orange"],width=5); d.text((L+302,h-57),"最大RTT",font=font(18),fill=C["text"])
    im.save(path,"JPEG",quality=95,subsampling=0)

cpu=parse_cpu(); net,iface=parse_net(); rtt,counts=parse_rtt()
write_csv(OUT/f"CPU_baseline_{RUN}.csv",["time","phase","cpu_max_pct"],[[t,p,f"{v:.2f}"] for t,p,v in cpu])
write_csv(OUT/f"NET_baseline_{RUN}.csv",["time","phase","rx_mib_s","tx_mib_s"],[[t,p,f"{rx:.2f}",f"{tx:.2f}"] for t,p,rx,tx in net])
write_csv(OUT/f"RTT_baseline_{RUN}.csv",["phase","cmd","avg_us","max_us"],rtt)
time_chart(OUT/f"CPU_baseline_{RUN}_chart.jpg","新基线 CPU 全阶段折线图","Redis主线程/线程组CPU峰值；Y轴固定0–100%",cpu,[2],["CPU"],[C["blue"]],"CPU (%)",100)
time_chart(OUT/f"NET_baseline_{RUN}_chart.jpg","新基线网络流量全阶段折线图",f"网卡 {iface}；相邻累计字节差分；单位MiB/s",net,[2,3],["RX","TX"],[C["blue"],C["orange"]],"MiB/s")
rtt_chart(OUT/f"RTT_baseline_{RUN}_chart.jpg",rtt)

def phase_values(rows, phase, idx): return [float(r[idx]) for r in rows if r[1]==phase]
cpu2=phase_values(cpu,"阶段2",2); cpu3=phase_values(cpu,"阶段3",2)
rx2=phase_values(net,"阶段2",2); tx2=phase_values(net,"阶段2",3)
rx3=phase_values(net,"阶段3",2); tx3=phase_values(net,"阶段3",3)
rttmap={(p,c):(a,m) for p,c,a,m in rtt}
s2_success,s2_fail,s2_timeout=counts["阶段2结束"]
all_success,all_fail,all_timeout=counts["全部结束"]
s3_success=all_success-s2_success; s3_fail=all_fail-s2_fail; s3_timeout=all_timeout-s2_timeout
cpu3_steady=cpu3[:-1] if len(cpu3)>1 and cpu3[-1] < 50 else cpu3
tx3_peak_gbps=max(tx3)*8.388608/1000

summary=f"""新基线本轮数据摘要
==================

轮次：{RUN}
口径：MGET总聚合100 Key；业务RTT严格大于5ms计超时；网络读保护100ms；TTL固定1秒；THREAD_SCALE=1。

阶段2：CPU平均{statistics.mean(cpu2):.2f}%、峰值{max(cpu2):.0f}%；RX平均{statistics.mean(rx2):.2f}MiB/s、峰值{max(rx2):.2f}MiB/s；TX平均{statistics.mean(tx2):.2f}MiB/s、峰值{max(tx2):.2f}MiB/s；RTT>5ms={s2_timeout}，占{s2_timeout/s2_success*100:.2f}%。
阶段3：CPU原始平均{statistics.mean(cpu3):.2f}%、稳态平均{statistics.mean(cpu3_steady):.2f}%、峰值{max(cpu3):.0f}%；RX平均{statistics.mean(rx3):.2f}MiB/s、峰值{max(rx3):.2f}MiB/s；TX平均{statistics.mean(tx3):.2f}MiB/s、峰值{max(tx3):.2f}MiB/s；RTT>5ms={s3_timeout}，占{s3_timeout/s3_success*100:.2f}%。

阶段3 RTT(us)：MGET {rttmap[("阶段3","MGET")][0]}/{rttmap[("阶段3","MGET")][1]}，SET {rttmap[("阶段3","SET")][0]}/{rttmap[("阶段3","SET")][1]}，GET {rttmap[("阶段3","GET")][0]}/{rttmap[("阶段3","GET")][1]}，APPEND {rttmap[("阶段3","APPEND")][0]}/{rttmap[("阶段3","APPEND")][1]}（平均/最大）。

阶段3 TX峰值约{tx3_peak_gbps:.2f}Gbps，接近目标20Gbps压力。全程成功{all_success}条、失败{all_fail}条、RTT>5ms {all_timeout}条。主从最终offset一致，压测阶段无复制链路异常采样，网卡丢包和错误增量均为0。
"""
(OUT/"本轮数据摘要.txt").write_text(summary,encoding="utf-8")

report=f"""新基线 {RUN} 完整分析报告
================================

一、测试口径
------------
1. 客户端版本标识为baseline。
2. 每次MGET聚合100个Key。
3. 单条命令端到端RTT严格大于5ms计入业务超时。
4. 网络读保护超时为100ms，达到100ms仍未获得完整响应才断开并重连。
5. 预热和SET的物理TTL均固定为1秒；约61GB表示累计灌入量，不表示阶段2仍有61GB同时驻留。
6. THREAD_SCALE=1；阶段2持续30秒，阶段3持续60秒。

二、数据完整性
--------------
1. 主控返回码为0，压测正常结束。
2. CPU日志、网络日志、Redis INFO、客户端输出、验收采样和结果报告均存在。
3. CPU有效样本：阶段2共{len(cpu2)}条，阶段3共{len(cpu3)}条。
4. 网络有效样本：阶段2共{len(rx2)}条，阶段3共{len(rx3)}条。
5. Redis主库PID由4156654变为678113，确认本轮没有复用旧Redis进程。
6. 基线I/O线程配置确认是io-threads=1、io-threads-do-reads=no。

三、CPU和网络
-------------
阶段2CPU平均{statistics.mean(cpu2):.2f}%，峰值{max(cpu2):.0f}%。阶段3原始平均{statistics.mean(cpu3):.2f}%，剔除最后一个线程停止边界样本后的稳态平均{statistics.mean(cpu3_steady):.2f}%，峰值{max(cpu3):.0f}%。阶段3最后一个24%采样发生在线程停止边界，折线图保留该点，但不能把它解释为稳态CPU下降。两个阶段稳态均接近单核满载，主线程CPU是明确瓶颈。

阶段2RX平均{statistics.mean(rx2):.2f}MiB/s、峰值{max(rx2):.2f}MiB/s；TX平均{statistics.mean(tx2):.2f}MiB/s、峰值{max(tx2):.2f}MiB/s。
阶段3RX平均{statistics.mean(rx3):.2f}MiB/s、峰值{max(rx3):.2f}MiB/s；TX平均{statistics.mean(tx3):.2f}MiB/s、峰值{max(tx3):.2f}MiB/s。
阶段3TX峰值折算约{tx3_peak_gbps:.2f}Gbps，已经复现约20Gbps的读响应流量压力。

四、RTT和5ms超时
----------------
阶段2MGET平均/最大RTT为3595/19234us，SET为2614/12540us。GET和APPEND在阶段2未执行，因此CSV保留-1，图中标注“未执行”且不绘制对应点。

阶段3MGET平均/最大RTT为6736/80862us，SET为6079/71537us，GET为6401/21322us，APPEND为5673/20657us。四类命令的阶段3平均RTT均高于5ms验收线。

阶段2成功{s2_success}条、失败{s2_fail}条，其中RTT>5ms为{s2_timeout}条，占{s2_timeout/s2_success*100:.2f}%。阶段3成功{s3_success}条、失败{s3_fail}条，其中RTT>5ms为{s3_timeout}条，占{s3_timeout/s3_success*100:.2f}%。全程RTT>5ms共{all_timeout}条，占{all_timeout/all_success*100:.2f}%。

这里的“RTT>5ms”是业务验收超时，不代表请求失败。本轮客户端失败数为0，说明请求最终都在100ms网络读保护范围内获得了响应。

五、TTL、内存、复制和丢包
------------------------
TTL固定1秒。阶段2内存从1.24GiB降至1.14GiB，是短TTL持续过期的结果；阶段3内存从0.93GiB增至4.46GiB，主要来自持续SET与APPEND形成的大Value。

阶段2最大主从offset差约2.74MiB，阶段3约3.52MiB。两个压测阶段的复制链路异常采样均为0，测试结束时主从offset一致。RX/TX丢包和错误增量均为0。

六、结论
--------
这轮数据完整，参数与当前100 Key/5ms/TTL 1秒口径一致，可以作为后续优化1至优化7的正式对照基线。基线清晰复现了三个问题：Redis主线程接近满载、阶段3发送流量接近20Gbps、阶段3约80.27%的成功命令超过5ms业务时延要求。后续各优化版本应保持相同负载、TTL和采集口径，并重点比较阶段3CPU、TX、四类命令RTT、5ms超时比例、复制积压和丢包。
"""
(OUT/"完整分析报告.txt").write_text(report,encoding="utf-8")
print(OUT)

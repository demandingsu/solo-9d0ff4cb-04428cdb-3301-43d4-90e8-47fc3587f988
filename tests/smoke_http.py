"""真实 HTTP 端到端冒烟脚本：通过运行中的服务验证完整业务闭环。

用法：先启动服务，再执行  python3 tests/smoke_http.py
地址可用环境变量覆盖：SMOKE_BASE=http://127.0.0.1:8000
"""
import json
import os
import urllib.request

BASE = os.environ.get("SMOKE_BASE", "http://127.0.0.1:8000")


def call(method: str, path: str, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


log = []


def check(name, cond, detail=""):
    log.append((name, cond, detail))
    print(f"[{'PASS' if cond else 'FAIL'}] {name} {detail}")


# 1. 开批
s, b = call("POST", "/batches", {"total_shards": 2, "source_cutoff_seq": 0})
check("开批", s == 201, str(b))
bid = b["batch_id"]

# 2. 分片提交 + 幂等 + 冲突
s, b = call("POST", f"/batches/{bid}/shards", {"shard_no": 1, "items": [
    {"outlet_no": "O1", "district": "A", "active": True},
    {"outlet_no": "O2", "district": "A", "active": False},
]})
check("片1首次提交", s == 201 and b["idempotent"] is False)
s, b = call("POST", f"/batches/{bid}/shards", {"shard_no": 1, "items": [
    {"outlet_no": "O1", "district": "A", "active": True},
    {"outlet_no": "O2", "district": "A", "active": False},
]})
check("片1同内容重试幂等", s == 201 and b["idempotent"] is True)
s, b = call("POST", f"/batches/{bid}/shards", {"shard_no": 1, "items": [
    {"outlet_no": "O1", "district": "B", "active": True},
]})
check("片1异内容冲突409", s == 409 and b["code"] == "SHARD_CONTENT_CONFLICT")

s, b = call("POST", f"/batches/{bid}/shards", {"shard_no": 2, "items": [
    {"outlet_no": "O3", "district": "B", "active": True},
]})
check("片2提交", s == 201)

# 3. 缺片状态
s, b = call("GET", "/batches/current")
check("进度查询", s == 200 and b["missing_shards"] == [])

# 4. 发布前查询为空
s, b = call("GET", "/outlets")
check("未发布前不可见", s == 200 and b["count"] == 0)

# 5. 首次发布
s, b = call("POST", f"/batches/{bid}/publish", {"catch_up_seq": 0})
check("首次发布v1", s == 200 and b["version"] == 1 and b["outlet_count"] == 3, str(b))

# 6. 变更：连续序号、缺号、幂等、冲突
s, b = call("POST", "/changes", {"seq": 1, "change_type": "move", "outlet_no": "O1", "district": "C"})
check("变更seq1生效", s == 201 and b["applied"] is True)
s, b = call("POST", "/changes", {"seq": 3, "change_type": "deactivate", "outlet_no": "O3"})
check("变更seq3越过缺号422", s == 422 and b["code"] == "SEQ_GAP")
s, b = call("POST", "/changes", {"seq": 1, "change_type": "move", "outlet_no": "O1", "district": "C"})
check("变更seq1重试幂等", s == 201 and b["idempotent"] is True)
s, b = call("POST", "/changes", {"seq": 1, "change_type": "move", "outlet_no": "O1", "district": "X"})
check("变更seq1异内容冲突", s == 409 and b["code"] == "CHANGE_CONTENT_CONFLICT")
s, b = call("POST", "/changes", {"seq": 2, "change_type": "deactivate", "outlet_no": "O3"})
check("变更seq2生效", s == 201 and b["applied"] is True)

# 7. 按区查询与计数一致
s, b = call("GET", "/outlets?district=C")
check("按区查询C", s == 200 and b["count"] == 1 and b["items"][0]["outlet_no"] == "O1")
s, stats = call("GET", "/stats")
check("计数", s == 200 and stats["total"] == 3 and stats["active"] == 1, json.dumps(stats, ensure_ascii=False))

# 8. 第二批全量上传期间，变更继续到达旧版
s, b = call("POST", "/batches", {"total_shards": 1, "source_cutoff_seq": 0})
bid2 = b["batch_id"]
call("POST", f"/batches/{bid2}/shards", {"shard_no": 1, "items": [
    {"outlet_no": "O1", "district": "A", "active": True},
    {"outlet_no": "O2", "district": "A", "active": False},
    {"outlet_no": "O3", "district": "B", "active": True},
    {"outlet_no": "O4", "district": "C", "active": True},
]})
s, b = call("POST", "/changes", {"seq": 3, "change_type": "add", "outlet_no": "O5",
                                  "district": "B", "active": True})
check("上传期间增量生效旧版", s == 201 and b["applied"] is True)
s, b = call("GET", "/stats")
check("旧版计数含O5", b["total"] == 4)

# 9. 发布门禁：缺号
s, b = call("POST", f"/batches/{bid2}/publish", {"catch_up_seq": 5})
check("缺号拒绝发布", s == 422 and b["code"] == "CHANGE_SEQ_GAP" and b["details"]["missing_seqs"] == [4, 5])

# 10. 补齐后发布 v2，窗口重放 seq 1-4（seq5 在窗口外，靠快照一致性校验）
s, b = call("POST", "/changes", {"seq": 4, "change_type": "deactivate", "outlet_no": "O4"})
check("seq4补齐", s == 201)
s, b = call("POST", f"/batches/{bid2}/publish", {"catch_up_seq": 4})
check("发布v2", s == 200 and b["version"] == 2 and b["changes_applied"] == 4, str(b))

s, outlets = call("GET", "/outlets")
omap = {o["outlet_no"]: o for o in outlets["items"]}
check("v2重放move", omap["O1"]["district"] == "C")
check("v2重放deactivate", omap["O3"]["active"] is False and omap["O4"]["active"] is False)
check("v2含新排口O4", "O4" in omap)

# 11. 水位不倒退
s, b = call("POST", "/batches", {"total_shards": 1, "source_cutoff_seq": 4})
bid3 = b["batch_id"]
call("POST", f"/batches/{bid3}/shards", {"shard_no": 1, "items": [
    {"outlet_no": "O1", "district": "C", "active": True},
]})
s, b = call("POST", f"/batches/{bid3}/publish", {"catch_up_seq": 2})
check("追平倒退409", s == 409 and b["code"] == "CATCHUP_REGRESSION")
call("POST", f"/batches/{bid3}/abort")

# 12. /docs 与 openapi
import urllib.request as u
docs = u.urlopen(BASE + "/docs", timeout=5).status
openapi = json.loads(u.urlopen(BASE + "/openapi.json", timeout=5).read().decode())
check("/docs可访问", docs == 200)
check("openapi路径齐全", {"/batches", "/changes", "/outlets", "/stats", "/health"}.issubset(openapi["paths"].keys()))

# 13. 截止序号基线：首批截止 100，101 号变更不得误报缺 1~100（缺陷回归场景）
s, b = call("POST", "/batches", {"total_shards": 1, "source_cutoff_seq": 100})
check("截止100开批", s == 201)
bid4 = b["batch_id"]
call("POST", f"/batches/{bid4}/shards", {"shard_no": 1, "items": [
    {"outlet_no": "N1", "district": "A", "active": True},
    {"outlet_no": "N2", "district": "A", "active": True},
]})
s, b = call("POST", "/changes", {"seq": 101, "change_type": "move", "outlet_no": "N1", "district": "B"})
# 现役 v2 尚无 N1（只存在于未发布快照）：变更被接收但成为链上断点，等待新快照发布时重放解除
check("101号变更被接收（现役无此排口->阻断待重放）",
      s == 201 and b["seq"] == 101 and b["applied"] is False and b["blocked"] is True
      and b["effective_baseline"] == 100 and b["expected_next_seq"] == 102, str(b))
s, b = call("POST", "/changes", {"seq": 103, "change_type": "deactivate", "outlet_no": "N2"})
check("真实缺号只报102", s == 422 and b["code"] == "SEQ_GAP" and b["details"]["missing_seqs"] == [102])
s, b = call("POST", "/changes", {"seq": 50, "change_type": "deactivate", "outlet_no": "N1"})
check("快照覆盖序号拒绝补写", s == 409 and b["code"] == "SEQ_COVERED_BY_SNAPSHOT")
s, b = call("GET", "/changes")
check("缺号视图不含1~100", b["missing_seqs"] == [] and b["next_expected_seq"] == 102)

# 14. 废弃后同截止序号重开：已接收变更（含阻断条目）保留，废弃不推进水位
s, b = call("POST", f"/batches/{bid4}/abort")
check("废弃批次", s == 200 and b["status"] == "aborted")
s, b = call("GET", "/health")
check("废弃不推进水位", b["version"] == 2 and b["last_served_seq"] == 4)
s, b = call("POST", "/batches", {"total_shards": 1, "source_cutoff_seq": 100})
check("同截止序号重开", s == 201)
bid5 = b["batch_id"]
call("POST", f"/batches/{bid5}/shards", {"shard_no": 1, "items": [
    {"outlet_no": "N1", "district": "A", "active": True},
    {"outlet_no": "N2", "district": "A", "active": True},
]})
s, b = call("GET", "/batches/current")
check("重开后下一应收102、断点保留", b["next_expected_seq"] == 102 and b["blocked"] is True)
# 追平到 101：窗口 (100,101] 只含 101，阻断在新快照上重放解除，发布成功；102 尚未要求
s, b = call("POST", f"/batches/{bid5}/publish", {"catch_up_seq": 101})
check("追平101发布成功并解除阻断", s == 200 and b["changes_applied"] == 1 and b["version"] == 3, str(b))
# 再想追平到 102 但批次已关 -> 拒绝（NO_OPEN_BATCH）；下一周期重开补齐 102
s, b = call("POST", f"/batches/{bid5}/publish", {"catch_up_seq": 102})
check("已关闭批次不可再发布", s in (404, 409) and b["code"] == "NO_OPEN_BATCH")
s, b = call("POST", "/batches", {"total_shards": 1, "source_cutoff_seq": 101})
check("下一周期截止101开批", s == 201)
bid6 = b["batch_id"]
call("POST", f"/batches/{bid6}/shards", {"shard_no": 1, "items": [
    {"outlet_no": "N1", "district": "B", "active": True},
    {"outlet_no": "N2", "district": "A", "active": True},
]})
# 102 缺失时追平 102 -> 真实缺号门禁（只报 102，不含历史）
s, b = call("POST", f"/batches/{bid6}/publish", {"catch_up_seq": 102})
check("缺102拒绝发布", s == 422 and b["code"] == "CHANGE_SEQ_GAP" and b["details"]["missing_seqs"] == [102])
# 补齐 102 后追平发布：102 纳入现役清单
call("POST", "/changes", {"seq": 102, "change_type": "deactivate", "outlet_no": "N2"})
s, b = call("POST", f"/batches/{bid6}/publish", {"catch_up_seq": 102})
check("补齐后追平发布", s == 200 and b["changes_applied"] == 1 and b["version"] == 4, str(b))
s, outlets = call("GET", "/outlets")
omap2 = {o["outlet_no"]: o for o in outlets["items"]}
check("阻断101迁区重放生效", omap2["N1"]["district"] == "B")
check("102停用生效", omap2["N2"]["active"] is False)
s, b = call("GET", "/health")
check("水位推进到102", b["last_served_seq"] == 102)
# 发布后增量按有效基线接续
s, b = call("POST", "/changes", {"seq": 103, "change_type": "move", "outlet_no": "N1", "district": "C"})
check("发布后增量103接续", s == 201 and b["applied"] is True and b["expected_next_seq"] == 104)

failed = [n for n, ok, _ in log if not ok]
print(f"\n==== 冒烟结果：{len(log) - len(failed)}/{len(log)} 通过 ====")
if failed:
    print("失败项：", failed)
    raise SystemExit(1)

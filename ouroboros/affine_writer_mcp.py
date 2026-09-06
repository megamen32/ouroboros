"""Audited narrow AFFiNE writer exposed as a local stdio MCP server."""
from __future__ import annotations
import hashlib, json, os, sys, time
from pathlib import Path
from typing import Any

SAFE_CATEGORIES={"inbox_note","task","journal"}
DATA_ROOT=Path(os.environ.get("OUROBOROS_DATA_ROOT","/home/roomhacker/Ouroboros/data"))
AUDIT_ROOT=DATA_ROOT/"affine-write-audit"
PROPOSALS=AUDIT_ROOT/"proposals"
AUDIT_LOG=AUDIT_ROOT/"audit.jsonl"
SETTINGS=DATA_ROOT/"settings.json"

TOOLS=[
 {"name":"propose_append","description":"Create an audited AFFiNE append proposal. Does not write AFFiNE.","inputSchema":{"type":"object","required":["category","workspaceId","docId","markdown","provenance"],"properties":{"category":{"type":"string","enum":sorted(SAFE_CATEGORIES)},"workspaceId":{"type":"string"},"docId":{"type":"string"},"markdown":{"type":"string","minLength":1},"provenance":{"type":"object","required":["source","message_id"],"properties":{"source":{"type":"string"},"message_id":{"type":"string"},"conversation_id":{"type":"string"},"received_at":{"type":["string","number"]}}}}}},
 {"name":"commit_append","description":"Commit a previously audited safe append proposal to AFFiNE.","inputSchema":{"type":"object","required":["proposalId"],"properties":{"proposalId":{"type":"string"}}}},
 {"name":"list_pending","description":"List pending AFFiNE write proposals.","inputSchema":{"type":"object","properties":{"limit":{"type":"integer","minimum":1,"maximum":100}}}},
]

def _jsonline(obj:dict[str,Any])->None:
 sys.stdout.write(json.dumps(obj,ensure_ascii=False,separators=(",",":"))+"\n"); sys.stdout.flush()

def _audit(event:dict[str,Any])->None:
 AUDIT_ROOT.mkdir(parents=True,exist_ok=True)
 row={"ts":time.time(),**event}
 with AUDIT_LOG.open("a",encoding="utf-8") as f: f.write(json.dumps(row,ensure_ascii=False)+"\n")

def _proposal_id(a:dict[str,Any])->str:
 basis=json.dumps({k:a[k] for k in ("category","workspaceId","docId","markdown","provenance")},ensure_ascii=False,sort_keys=True,separators=(",",":"))
 return hashlib.sha256(basis.encode()).hexdigest()[:24]

def _propose(a:dict[str,Any])->dict[str,Any]:
 category=str(a.get("category") or "")
 if category not in SAFE_CATEGORIES: raise ValueError("category is not auto-writable")
 for k in ("workspaceId","docId","markdown"):
  if not isinstance(a.get(k),str) or not a[k].strip(): raise ValueError(f"{k} is required")
 prov=a.get("provenance")
 if not isinstance(prov,dict) or not str(prov.get("source") or "").strip() or not str(prov.get("message_id") or "").strip(): raise ValueError("provenance.source and provenance.message_id are required")
 pid=_proposal_id(a); PROPOSALS.mkdir(parents=True,exist_ok=True); p=PROPOSALS/f"{pid}.json"
 if p.exists():
  d=json.loads(p.read_text()); return {"ok":True,"proposalId":pid,"status":d.get("status","proposed"),"deduplicated":True}
 d={"proposalId":pid,"status":"proposed","created_at":time.time(),**{k:a[k] for k in ("category","workspaceId","docId","markdown","provenance")}}
 p.write_text(json.dumps(d,ensure_ascii=False,indent=2)+"\n"); _audit({"event":"proposal_created","proposalId":pid,"category":category,"provenance":prov})
 return {"ok":True,"proposalId":pid,"status":"proposed","deduplicated":False}

def _manager():
 from ouroboros.mcp_client import MCPManager
 settings=json.loads(SETTINGS.read_text())
 for s in settings.get("MCP_SERVERS",[]):
  if s.get("id")=="affine": s["allowed_tools"]=["append_markdown"]
 m=MCPManager(); m.reconfigure(settings); r=m.refresh_server("affine")
 if not r.get("ok"): raise RuntimeError("AFFiNE MCP unavailable")
 return m

def _commit(a:dict[str,Any])->dict[str,Any]:
 pid=str(a.get("proposalId") or "").strip()
 if not pid or not all(c in "0123456789abcdef" for c in pid): raise ValueError("invalid proposalId")
 p=PROPOSALS/f"{pid}.json"
 if not p.exists(): raise ValueError("proposal not found")
 d=json.loads(p.read_text())
 if d.get("status")=="committed": return {"ok":True,"proposalId":pid,"status":"committed","deduplicated":True,"result":d.get("result")}
 if d.get("category") not in SAFE_CATEGORIES: raise ValueError("proposal category is not auto-writable")
 prov=d["provenance"]
 footer=f"\n\n---\n_Source: {prov.get('source')} · message_id: {prov.get('message_id')}"
 if prov.get("conversation_id"): footer+=f" · conversation_id: {prov.get('conversation_id')}"
 footer+="_\n"
 args={"workspaceId":d["workspaceId"],"docId":d["docId"],"markdown":d["markdown"].rstrip()+footer}
 m=_manager(); out=m.call_tool("mcp_affine__append_markdown",args)
 if "MCP_TOOL_ERROR" in str(out):
  _audit({"event":"commit_failed","proposalId":pid,"error":str(out)[:1000]}); raise RuntimeError("AFFiNE append failed")
 d["status"]="committed"; d["committed_at"]=time.time(); d["result"]=str(out)[:4000]; p.write_text(json.dumps(d,ensure_ascii=False,indent=2)+"\n")
 _audit({"event":"committed","proposalId":pid,"category":d["category"],"provenance":prov,"content_sha256":hashlib.sha256(args["markdown"].encode()).hexdigest()})
 return {"ok":True,"proposalId":pid,"status":"committed"}

def _pending(a:dict[str,Any])->dict[str,Any]:
 limit=max(1,min(100,int(a.get("limit") or 50))); rows=[]
 if PROPOSALS.exists():
  for p in sorted(PROPOSALS.glob("*.json"),key=lambda x:x.stat().st_mtime,reverse=True):
   d=json.loads(p.read_text())
   if d.get("status")=="proposed": rows.append({k:d.get(k) for k in ("proposalId","category","workspaceId","docId","provenance","created_at")})
   if len(rows)>=limit: break
 return {"ok":True,"proposals":rows}

def dispatch(method:str,params:dict[str,Any])->dict[str,Any]:
 if method=="initialize": return {"protocolVersion":str(params.get('protocolVersion') or '2025-06-18'),"serverInfo":{"name":"ouroboros-affine-writer","version":"1.0.0"},"capabilities":{"tools":{"listChanged":False}}}
 if method=="ping": return {}
 if method=="tools/list": return {"tools":TOOLS}
 if method=="tools/call":
  name=str(params.get("name") or ""); a=params.get("arguments") or {}
  if name=="propose_append": result=_propose(a)
  elif name=="commit_append": result=_commit(a)
  elif name=="list_pending": result=_pending(a)
  else: raise ValueError("unknown tool")
  return {"content":[{"type":"text","text":json.dumps(result,ensure_ascii=False)}],"structuredContent":result,"isError":False}
 raise KeyError("method not found")

def main()->int:
 for raw in sys.stdin:
  try:
   req=json.loads(raw); rid=req.get("id"); method=req.get("method")
   if rid is None: continue
   try: result=dispatch(method,req.get("params") or {}); _jsonline({"jsonrpc":"2.0","id":rid,"result":result})
   except KeyError as e: _jsonline({"jsonrpc":"2.0","id":rid,"error":{"code":-32601,"message":str(e)}})
   except Exception as e: _audit({"event":"tool_error","method":method,"error":str(e)[:1000]}); _jsonline({"jsonrpc":"2.0","id":rid,"error":{"code":-32000,"message":str(e)}})
  except Exception: continue
 return 0
if __name__=="__main__": raise SystemExit(main())

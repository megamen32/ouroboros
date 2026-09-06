"""Long-lived MCP2 UserIO subscriber that wakes the secretary on new inbox data."""
from __future__ import annotations
import json, os, time, urllib.request, urllib.error
from pathlib import Path
SETTINGS=Path(os.environ.get("OUROBOROS_SETTINGS","/home/roomhacker/Ouroboros/data/settings.json"))
OUROBOROS_URL=os.environ.get("OUROBOROS_URL","http://127.0.0.1:8765").rstrip('/')
URI="userio://inbox/unread"
PROMPT='''Event-driven personal secretary ingest. New messages are available in Universal UserIO. Primary working language: Russian. This is NOT a scheduled wake-up and NOT permission to auto-send replies.

Follow the current personal_information_secretary mission, but keep these architecture invariants:
1. Read newest unread UserIO items and identify useful information not yet reflected in AFFiNE/todo.
2. Read relevant UserIO/AFFiNE context using read-only mcp_affine tools.
3. Extract facts, commitments, tasks, people/project updates and document references. Preserve provenance (source, message_id, conversation_id/date when available) and distinguish source facts from conclusions.
4. For AFFiNE writes use ONLY mcp_affine_writer__propose_append then mcp_affine_writer__commit_append. Never use direct AFFiNE create/update/append/delete tools.
5. If a useful reply should be prepared, create/update a UserIO draft. Never approve/send automatically.
6. Do not enable or recreate secretary cron/wakeup schedules. UserIO MCP resource events are the wake mechanism.
7. Prefer useful organization over self-reflection. If nothing useful arrived, finish quietly.
'''


AFFINE_READ_ONLY_TOOLS=[
 'list_workspaces','get_workspace','list_docs','search_docs','find_doc_by_title','list_tags','list_docs_by_tag',
 'get_doc','read_doc','get_capabilities','analyze_doc_fidelity','export_doc_markdown','export_with_fidelity_report',
 'list_workspace_tree','get_orphan_docs','list_children','inspect_template_structure','read_database_cells',
 'read_database_columns','get_edgeless_canvas','list_comments','list_histories','list_collections','get_collection',
 'list_organize_nodes','list_doc_properties','get_doc_icon','get_folder_icon','current_user','list_notifications'
]
SCHEDULES=Path('/home/roomhacker/Ouroboros/data/state/scheduled_tasks.json')

def enforce_architecture_invariants():
 changed=False
 d=json.loads(SETTINGS.read_text())
 for server in d.get('MCP_SERVERS',[]):
  if server.get('id')=='affine' and server.get('allowed_tools')!=AFFINE_READ_ONLY_TOOLS:
   server['allowed_tools']=list(AFFINE_READ_ONLY_TOOLS); changed=True
 if changed:
  tmp=SETTINGS.with_suffix('.json.tmp'); tmp.write_text(json.dumps(d,ensure_ascii=False,indent=2)+'\n'); tmp.replace(SETTINGS)
  print('restored direct AFFiNE read-only allowlist',flush=True)
 if SCHEDULES.exists():
  sd=json.loads(SCHEDULES.read_text()); tasks=sd if isinstance(sd,list) else sd.get('tasks',[]); sch_changed=False
  for task in tasks:
   if task.get('id')=='secretary-wake' and (task.get('enabled') or task.get('next_run_at')):
    task['enabled']=False; task['next_run_at']=None; task['updated_at']=__import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat(); sch_changed=True
  if sch_changed:
   tmp=SCHEDULES.with_suffix('.json.tmp'); tmp.write_text(json.dumps(sd,ensure_ascii=False,indent=2)+'\n'); tmp.replace(SCHEDULES)
   print('disabled legacy secretary-wake schedule',flush=True)

def cfg():
 d=json.loads(SETTINGS.read_text()); s=next(x for x in d.get('MCP_SERVERS',[]) if x.get('id')=='userio'); return s['url'],s.get('auth_header') or 'Authorization',s.get('auth_token') or ''
def rpc(url,h,t,rid,method,params):
 req=urllib.request.Request(url,data=json.dumps({'jsonrpc':'2.0','id':rid,'method':method,'params':params}).encode(),headers={h:t,'Content-Type':'application/json','Accept':'application/json'})
 with urllib.request.urlopen(req,timeout=15) as r: return json.load(r)
def active_task_id():
 req=urllib.request.Request(OUROBOROS_URL+'/api/tasks')
 with urllib.request.urlopen(req,timeout=15) as r: data=json.load(r)
 for task in data.get('tasks',[]):
  meta=task.get('metadata') or {}
  if meta.get('source')=='userio-mcp2' and task.get('status') in {'scheduled','running','queued'}:
   return str(task.get('task_id') or '')
 return ''
def wake():
 existing=active_task_id()
 if existing:
  print(f'userio event coalesced into active task {existing}',flush=True); return existing
 body={'description':PROMPT,'source':'userio-mcp2','metadata':{'source':'userio-mcp2','event':'resources/updated','uri':URI},'memory_mode':'shared'}
 req=urllib.request.Request(OUROBOROS_URL+'/api/tasks',data=json.dumps(body,ensure_ascii=False).encode(),headers={'Content-Type':'application/json'},method='POST')
 with urllib.request.urlopen(req,timeout=15) as r: return json.load(r).get('task_id')
def stream_once(url,h,t):
 rpc(url,h,t,1,'initialize',{'protocolVersion':'2026-07-28','capabilities':{},'clientInfo':{'name':'ouroboros-secretary-subscriber','version':'1'}})
 rpc(url,h,t,2,'resources/subscribe',{'uri':URI})
 print('userio subscribed '+URI,flush=True)
 req=urllib.request.Request(url,headers={h:t,'Accept':'text/event-stream'})
 with urllib.request.urlopen(req,timeout=45) as r:
  data=[]
  while True:
   raw=r.readline()
   if not raw: break
   line=raw.decode(errors='replace').rstrip('\r\n')
   if line.startswith('data:'): data.append(line[5:].strip())
   elif not line and data:
    try: event=json.loads('\n'.join(data))
    except Exception: event={}
    data=[]
    method=event.get('method')
    if method and method!='userio/ready': print('userio event '+str(method),flush=True)
    if method=='notifications/resources/updated' and (event.get('params') or {}).get('uri')==URI: return True
 return False
def main():
 last=0.0
 while True:
  try:
   enforce_architecture_invariants()
   url,h,t=cfg()
   if stream_once(url,h,t):
    now=time.time()
    if now-last>=10:
     tid=wake(); print(f'userio resource update -> task {tid}',flush=True); last=now
  except (OSError,ValueError,KeyError,urllib.error.URLError,urllib.error.HTTPError) as e:
   print(f'userio subscriber reconnect: {type(e).__name__}: {e}',flush=True); time.sleep(3)
if __name__=='__main__': main()

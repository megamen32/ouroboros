"""Long-lived MCP2 UserIO subscriber that wakes the secretary on new inbox data."""
from __future__ import annotations
import json, os, time, urllib.request, urllib.error
from pathlib import Path
SETTINGS=Path(os.environ.get("OUROBOROS_SETTINGS","/home/roomhacker/Ouroboros/data/settings.json"))
OUROBOROS_URL=os.environ.get("OUROBOROS_URL","http://127.0.0.1:8765").rstrip('/')
URI="userio://inbox/unread"
PROMPT='''Event-driven personal secretary ingest. New messages are available in Universal UserIO. Primary working language: Russian.

Follow personal_information_secretary.md as the source of truth. Old cron-secretary/curfew/digest/mandate rules are deprecated.
1. Read newest unread UserIO items and relevant Telegram/UserIO/AFFiNE context.
2. Prioritize owner's DMs, «ИИ Frontier», «ИИ бенчмарки», Artem Popov and Oleg Karpov when relevant, then other conversations.
3. Create/update useful AFFiNE artifacts: people profiles, meetings, projects, agreements, ideas and links. Preserve provenance and distinguish source facts from conclusions.
4. Create/update todo cards for explicit commitments, next steps, deadlines and reminders; deduplicate.
5. If a useful reply should be prepared, create/update a UserIO draft. Never approve/send automatically.
6. Direct AFFiNE create/update tools are allowed by the owner's current MCP policy. Do not rewrite MCP allowlists or schedules from this subscriber.
7. Scheduled secretary-wake and event-driven UserIO wakeups are complementary: keep cron enabled and coalesce only duplicate active secretary work.
8. Prefer useful organization over infrastructure self-checks or self-reflection. If nothing useful arrived, finish quietly.
'''



def cfg():
 d=json.loads(SETTINGS.read_text()); s=next(x for x in d.get('MCP_SERVERS',[]) if x.get('id')=='userio'); return s['url'],s.get('auth_header') or 'Authorization',s.get('auth_token') or ''
def rpc(url,h,t,rid,method,params):
 req=urllib.request.Request(url,data=json.dumps({'jsonrpc':'2.0','id':rid,'method':method,'params':params}).encode(),headers={h:t,'Content-Type':'application/json','Accept':'application/json'})
 with urllib.request.urlopen(req,timeout=15) as r: return json.load(r)
def active_task_id():
 req=urllib.request.Request(OUROBOROS_URL+'/api/tasks')
 with urllib.request.urlopen(req,timeout=15) as r: data=json.load(r)
 for task in data.get('tasks',[]):
  if task.get('status') not in {'scheduled','running','queued'}:
   continue
  meta=task.get('metadata') or {}
  purpose=str(meta.get('purpose') or '')
  schedule_id=str(meta.get('schedule_id') or task.get('schedule_id') or '')
  source=str(meta.get('source') or task.get('source') or '')
  description=str(task.get('description') or '').lower()
  if (source=='userio-mcp2' or schedule_id=='secretary-wake' or
      'personal_information_secretary' in purpose or 'секретар' in description):
   return str(task.get('task_id') or task.get('root_task_id') or '')
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
   url,h,t=cfg()
   if stream_once(url,h,t):
    now=time.time()
    if now-last>=10:
     tid=wake(); print(f'userio resource update -> task {tid}',flush=True); last=now
  except (OSError,ValueError,KeyError,urllib.error.URLError,urllib.error.HTTPError) as e:
   print(f'userio subscriber reconnect: {type(e).__name__}: {e}',flush=True); time.sleep(3)
if __name__=='__main__': main()

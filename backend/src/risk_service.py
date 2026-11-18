import os
import redis
from sqlalchemy import select
from .models import Accession,Isolation,RiskGroup,RiskAssessment
from .common import get,reject,finish
from .catalog_service import latest_observation,recent_observations
queue=redis.Redis.from_url(os.getenv('REDIS_URL','redis://redis:6379/0'),decode_responses=True)
def current_group(db,accession_id):
    # 最新一组报审且覆盖当前最新复测才有效；新复测登记后旧组自动失效。
    obs=latest_observation(db,accession_id)
    if not obs:return None
    group=db.scalar(select(RiskGroup).where(RiskGroup.accession_id==accession_id).order_by(RiskGroup.id.desc()).limit(1))
    if not group:return None
    top=db.scalar(select(RiskAssessment.observation_id).where(RiskAssessment.group_id==group.id).order_by(RiskAssessment.observation_id.desc()).limit(1))
    return group if top==obs.id else None
def reviewed_group(db,accession_id):
    group=current_group(db,accession_id)
    return group if group and group.status=='reviewed' else None
def request_risk(db,data,user):
    folio=get(db,Accession,data.accession_id,True)
    observations=recent_observations(db,folio.id,2)
    reject(not observations,'先录入病害测量')
    reject(not db.scalar(select(Isolation).where(Isolation.accession_id==folio.id,Isolation.active==True)),'先隔离入柜再评估')
    key=','.join(str(o.id) for o in sorted(observations,key=lambda o:o.id))
    existing=db.scalar(select(RiskGroup).where(RiskGroup.accession_id==folio.id,RiskGroup.obs_key==key))
    if existing:
        reject(existing.status!='failed','同组复测已送审，重试不会重复生成分级')
        existing.status='queued';existing.error=''
        result=finish(db,user,'重试风险报审',existing)
        queue.rpush('folio:risk',existing.id);return result
    group=RiskGroup(accession_id=folio.id,obs_key=key);db.add(group);db.flush()
    for obs in observations:db.add(RiskAssessment(group_id=group.id,accession_id=folio.id,observation_id=obs.id))
    db.flush();result=finish(db,user,'提交风险报审',group)
    queue.rpush('folio:risk',group.id);return result
def review(db,id,data,user):
    group=get(db,RiskGroup,id)
    get(db,Accession,group.accession_id,True)
    group=get(db,RiskGroup,id,True)
    reject(group.status!='done','仅已完成分级可复核')
    current=current_group(db,group.accession_id)
    reject(not current or current.id!=group.id,'病害已复测，请重新报审最新一组')
    predictions={a.predicted for a in db.scalars(select(RiskAssessment).where(RiskAssessment.group_id==group.id))}
    reject(len(predictions)>1 and len(data.note.strip())<2,'两次分级不同，主管须写明裁决理由')
    group.status='reviewed';group.approved=data.level;group.review_note=data.note.strip()
    return finish(db,user,'风险主管复核',group)

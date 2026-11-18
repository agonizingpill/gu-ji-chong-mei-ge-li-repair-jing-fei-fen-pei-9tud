import time,logging,json
from pathlib import Path
from sqlalchemy import select
from .database import Session
from .models import RiskGroup,RiskAssessment,Observation
from .risk_service import queue
from .algorithm import classify
logging.basicConfig(level=logging.INFO)
while True:
    Path('/tmp/worker-heartbeat').touch()
    try:
        item=queue.blpop('folio:risk',timeout=2)
        if not item:continue
        with Session() as db:
            group=db.get(RiskGroup,int(item[1]))
            if not group or group.status!='queued':continue
            group.status='running';db.commit()
            try:
                members=db.scalars(select(RiskAssessment).where(RiskAssessment.group_id==group.id).order_by(RiskAssessment.observation_id)).all()
                for member in members:
                    obs=db.get(Observation,member.observation_id)
                    member.predicted,member.metrics=classify(obs.humidity,obs.holes,obs.mold)
                group.status='done';group.error=''
                Path('/artifacts').mkdir(exist_ok=True)
                Path(f'/artifacts/risk-{group.id}.json').write_text(json.dumps({'group_id':group.id,'obs_key':group.obs_key,'predictions':{str(m.observation_id):m.predicted for m in members},'metrics':members[-1].metrics},ensure_ascii=False))
            except Exception as exc:group.status='failed';group.error=str(exc)[:200]
            db.commit()
    except Exception:logging.exception('worker iteration');time.sleep(1)

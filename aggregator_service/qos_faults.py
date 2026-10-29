"""Generador de métrica QoS / desviaciones internas del microservicio.

Se activa de forma independiente en CADA contenedor, solo al consultar /metrics.
La aplicación de despliegue no provoca fallos y desconoce los datos funcionales.
"""
import json
import os
import random
import threading
import time

METRICS=('response_time_ms','execution_time','availability','reliability','cost')

class FaultProfile:
    def __init__(self):
        self.lock=threading.RLock()
        self.rng=random.Random()
        self.allowed=set(METRICS)
        self.limits=json.loads(os.getenv('QOS_LIMITS_JSON','{}'))
        self.low=float(os.getenv('QOS_FAULT_MIN_SECONDS','30'))
        self.high=max(self.low,float(os.getenv('QOS_FAULT_MAX_SECONDS','55')))
        self.probability=float(os.getenv('QOS_FAULT_PROBABILITY','0.35'))
        self.next_at=time.monotonic()+self.rng.uniform(self.low,self.high)
        self.expires_at=0.0
        self.values={}
        self.last_slot=None
        self.jitter={}
        self.variant=os.getenv('SERVICE_VARIANT','standard')

    def inject(self,payload):
        with self.lock:
            for key in self.allowed:
                if key in payload:
                    v=payload[key]
                    if type(v) not in (int,float):raise ValueError(f'{key} debe ser numérico')
                    self.values[key]=float(v)
            self.expires_at=time.monotonic()+max(1,min(float(payload.get('recover_after_seconds',18)),600))
            return {'status':'accepted','metrics_affected':list(self.values)}

    def decorate(self,metrics):
        with self.lock:
            now=time.monotonic()
            if self.expires_at and now>=self.expires_at:
                self.values={};self.expires_at=0.0
            if now>=self.next_at and not self.values:
                options=[(name,rule) for name,rule in self.limits.items()
                         if name in self.allowed and isinstance(rule,dict) and ('max' in rule or 'min' in rule)]
                if options and self.rng.random() <= self.probability:
                    key,rule=self.rng.choice(options)
                    if 'max' in rule:
                        self.values[key]=round(float(rule['max'])*self.rng.uniform(1.7,2.2)+.001,3)
                    else:
                        self.values[key]=round(float(rule['min'])*self.rng.uniform(.18,.45),3)
                    self.expires_at=now+self.rng.uniform(16,24)
                self.next_at=now+self.rng.uniform(self.low,self.high)
            slot=int(now/2)
            if slot!=self.last_slot:
                self.last_slot=slot
                self.jitter={k:self.rng.uniform(.85,1.15) for k in self.allowed}
            factor={'standard':1.,'fast':.70,'eco':.89}.get(self.variant,1.)
            out=dict(metrics)
            for k in self.allowed:
                v=out.get(k)
                if type(v) in (int,float):
                    multiplier=(1/factor if k=='availability' else factor)
                    out[k]=round(max(0.,v*self.jitter[k]*multiplier),3)
            out.update(self.values)
            return out

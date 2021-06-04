import time

from job import Job
from slurm_dispatcher import SlurmDispatcher

dispatcher = SlurmDispatcher(queue="shared-gpu")

job1 = Job(dispatcher=dispatcher, name="hello_world1", job="hello_world1.py", nodes=2, walltime="00:10:00")
job1.submit()
print("launch job %s, job id %s, status %s" %(job1.name, job1.job_id, job1.status))
time.sleep(10)
print("job %s new status %s" %(job1.job_id, job1.check_status()))

job2 = Job(dispatcher=dispatcher, name="hello_world2", job="hello_world2.py", nodes=1, walltime="00:10:00", dependent_jobs=job1)
job2.submit()
print("launch job %s, job id %s, status %s" %(job2.name, job2.job_id, job2.status))
print("job %s new status %s" %(job2.job_id, job2.check_status()))

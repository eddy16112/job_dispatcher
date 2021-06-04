from enum import Enum

class JobStatus(Enum):
  Created = 1
  Submitted = 2
  Running = 3
  Finished = 4

class Job(object):
  __slots__ = ["dispatcher", "name", "job", "executor", "nodes", "walltime", "job_id", "status", "dependent_jobs"]
  def __init__(self,
               dispatcher=None,
               name=None,
               job=None,
               executor="python",
               nodes=None,
               walltime=None, 
               dependent_jobs=None):
    self.dispatcher = dispatcher
    self.name = name
    self.job = job
    self.executor = executor
    self.nodes = nodes
    self.walltime = walltime
    self.job_id = None
    self.status = JobStatus.Created

    # save depdendent jobs
    self.dependent_jobs = []
    if dependent_jobs == None:
      pass
    elif type(dependent_jobs) is Job:
      self.dependent_jobs.append(dependent_jobs)
    elif type(dependent_jobs) is list:
      self.dependent_jobs.extend(dependent_jobs)
    else:
      assert 0, "Incorrect type of dependent_jobs"  

  def submit(self):
    self.job_id = self.dispatcher.dispatch(self)
    self.status = JobStatus.Submitted

  def check_status(self):
    self.status = self.dispatcher.check_status(self)
    return self.status
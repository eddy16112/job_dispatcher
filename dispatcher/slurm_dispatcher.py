import os, subprocess
from enum import Enum

from job import JobStatus

class SlurmDispatcher(object):
  def __init__(self,
               queue=None,
               project=None):
    assert queue != None, "queue is None"
    self.queue = queue
    self.project = project
    
  def dispatch(self, job): 
    # check
    assert job.status == JobStatus.Created, "Job is already submitted"

    # parse dependent jobs
    if len(job.dependent_jobs) == 0:
      dependencies = None
    else:
      dependencies = ""
      for j in job.dependent_jobs:
        dependencies = dependencies + ":" + j.job_id

    # create tmp job bash file for sbatch
    filename = job.name + ".sh"
    job_file = open(filename, "w")
    job_file.writelines("#!/usr/bin/env bash\n")
    job_file.writelines("\n")
    job_file.writelines("#SBATCH -J %s\n" % job.name)
    job_file.writelines("#SBATCH -n %s\n" % job.nodes)
    job_file.writelines("#SBATCH -t %s\n" % job.walltime)
    job_file.writelines("\n")
    job_file.writelines("srun -n %s %s %s" %(job.nodes, job.executor, job.job))
    job_file.close()
    
    # submit job using sbatch
    if dependencies == None:
      sbatch_cmd = "sbatch --parsable -N %s %s" %(job.nodes, filename)
    else:
      sbatch_cmd = "sbatch --parsable -N %s --dependency=aftercorr%s %s" %(job.nodes, dependencies, filename)
    print(sbatch_cmd)
    proc = subprocess.Popen(sbatch_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    proc.wait()
    lines = proc.stdout.readlines()
    assert len(lines) == 1, "Incorrect output %s" %(len(lines))
    proc.stdout.close()
    job_id = lines[0].decode("utf-8").strip()

    os.system("rm -rf %s" %filename)
    return job_id
    
  def check_status(self, job):
    squeue_cmd = "squeue -j %s" % job.job_id
    print(squeue_cmd)
    proc = subprocess.Popen(squeue_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    proc.wait()
    lines = proc.stdout.readlines()
    proc.stdout.close()
    if len(lines) == 2:
      line = lines[1].decode("utf-8").strip().split()
      slurm_status = line[4]
      if slurm_status == "PD":
        status = JobStatus.Submitted
      elif slurm_status == "R":
        status = JobStatus.Running
      elif slurm_status == "CG":
        status = JobStatus.Finished
      else:
        assert 0, "Unkown status %s" % slurm_status
    elif len(lines) == 1:
      status = JobStatus.Finished
    else:
      assert 0, "Incorrect output %s" %(len(lines))

    return status

    

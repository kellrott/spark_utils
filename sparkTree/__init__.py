
import sys
import os
from pyspark import SparkContext

class Stack(object):

    def __init__(self, target):
        self.target = target

    def connect(self, spark_host, job_name):
        self.spark_host = spark_host
        self.job_name = job_name
        self.spark_context = SparkContext(spark_host, job_name)

    @staticmethod
    def addJobTreeOptions(parser):
        parser.add_option("--batchSystem", dest="batchSystem",
                      help="This is an old flag that is kept to maintain compatibility default=%default",
                      default="spark")


        parser.add_option("--jobTree", dest="jobTree", 
                      help="This is an old flag that it is maintained for compatibility",
                      default=None)
    


    def startJobTree(self, options):
        self.options = options
        extra_path = os.path.dirname(os.path.abspath(sys.argv[0]))
        os.environ['PYTHONPATH'] = os.environ.get('PYTHONPATH', "") + ":" + extra_path

        #print "Starting"

        sm = StackManager(self.spark_context)
        targets = self.spark_context.parallelize([('start', self.target)]) 
        sm.runTargetList(targets)



def job_run( target_tuple ):
    (name, target) = target_tuple
    target.__manager__ = TargetManager()
    target.run()
    return { 'target' : target.__manager__.child_list, 'follow' : target.__manager__.follow_list }


class TargetManager(object):
    def __init__(self):
        self.child_list = []
        self.follow_list = []

    def _addTarget(self, name, target):
        self.child_list.append( (name, target) )

    def _addFollowTarget(self, name, target):
        self.follow_list.append( (name, target) )


class StackManager(object):

    def __init__(self, spark_context ):
        self.spark_context = spark_context

    def runTargetList(self, target_list):
        requests = target_list.map( job_run )
        child_jobs = requests.filter( lambda x : len(x['target']) ).flatMap( lambda x: x['target'] )
        #print child_jobs.collect()
        if child_jobs.count() > 0:
            self.runTargetList(child_jobs)

        follow_jobs = requests.filter( lambda x : len(x['follow']) ).flatMap( lambda x: x['follow'] )
        #print follow_jobs.collect()
        if follow_jobs.count() > 0:
            self.runTargetList(follow_jobs)





class Target(object):
    """
    The target class describes a python class to be pickel'd and 
    run as a remote process at a later time.
    """

    def __init__(self, **kw):
        self.child_count = 0

    def run(self):
        """
        The run method is user provided and run in a seperate process,
        possible on a remote node.
        """
        raise Exception()

   
    def addChildTarget(self, child_name, child=None):
        """
        Add child target to be executed
        
        :param child_name:
            Unique name of child to be executed
        
        :param child:
            Target object to be pickled and run as a remote 
        
        
        Example::
            
            class MyWorker(jobtree.Target):
                
                def __init__(self, dataBlock):
                    self.dataBlock = dataBlock
                
                def run(self):
                    c = MyOtherTarget(dataBlock.pass_to_child)
                    self.addChildTarget('child', c )
        
        
        """
        if isinstance(child_name, Target):
            self.__manager__._addTarget("child_%d" % (self.child_count), child_name)
            self.child_count += 1
        else:
            self.__manager__._addTarget(child_name, child)
   
    def runTarget(self, child):
        """
        Run a target directly (inline). This method give the manager a chance
        to setup table and run path info for the target class before calling the 'run' method
        
        """
        
        self.__manager__._runTarget(self, child)
    
    def setFollowOnTarget(self, child_name):
        if isinstance(child_name, Target):
            self.addFollowTarget("follow", child_name)
        else:
            self.addFollowTarget(child_name, child)

    def addFollowTarget(self, child_name, child):
        """
        A follow target is a delayed callback, that isn't run until all 
        of a targets children have complete
        """
      
        self.__manager__._addFollowTarget(child_name, child)


import unittest
import os
import sys
import subprocess
import shutil
import time
import config_test

from jobTree.scriptTree.target import Target
from jobTree.scriptTree.stack import Stack

class Child_wait(Target):
    def run(self):
        print "Wait Child!!!"

class Child_follow(Target):
    def __init__(self, table):
        self.table = table
    
    def run(self):
        print "Follow Child!!!"

class Child_parent(Target):
    def __init__(self):
        pass
    
    def run(self):
        self.addChildTarget('child_a', Child_wait())
        self.addFollowTarget('follow_a', Child_follow('child_a/output'))

class Submit(Target):

    def run(self):        
        child1 = Child_wait()        
        self.addChildTarget('child_1', child1)
        self.addFollowTarget('follow_1', Child_follow('child_1/output'))
        self.addFollowTarget('a_follow_1', Child_follow('child_1/output'))
        
        self.addChildTarget('child_2', Child_parent())
        self.addFollowTarget('follow_2', Child_follow('child_2/child_a/output'))

class TestCase(unittest.TestCase):    
    def test_submit(self):

        s = Stack(Submit())
        options = {}
        s.connect(config_test.SPARK_SERVER, "follow_test")
        failed = s.startJobTree(options)

    def tearDown(self):
        self.clear()
    
    def clear(self):
        return
        try:
            shutil.rmtree( "tmp_dir" )
        except OSError:
            pass
        try:
            shutil.rmtree( "data_dir" )
        except OSError:
            pass

            
def main():
    import test_follow
    s = Stack(test_follow.Submit())
    s.connect(config_test.SPARK_SERVER, "follow_test")
    options = {}
    failed = s.startJobTree(options)

if __name__ == '__main__':
    main()
from job import *

j = Job('test')
s = Step('sql1',"SQL",'sample.sql','Y','err')
j.steps.append(s)
s = Step('sql2',"SQL",'sample2.sql','Y','err')
j.steps.append(s)
s = Step('mod1',"python","imported",'Y','err')
j.steps.append(s)
s = Step('mod2',"python","imported",'Y','err')
j.steps.append(s)
s = Step('mod2',"python","imported",'Y','err')
s.wait_on('sql2')
j.steps.append(s)
j.execute()


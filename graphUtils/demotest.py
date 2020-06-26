from userService.graphUtils import graphCU, graphEva, graphService
'''
    ##### test #####
'''
cuUtil = graphCU.GraphCU()
evaUtil = graphEva.GraphEva()
serviceUtil = graphService.GraphService()

rt = serviceUtil.networkSearch(value='person_1')
print(rt)
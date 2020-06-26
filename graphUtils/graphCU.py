from py2neo import Graph

url = 'localhost:7474'
username = 'neo4j'
password = 'neo4j'


'''
    ##### 实现graph的 增加/更新 节点/关系 ######
    createRel: 创建person实体间的关系
    createRmt:  创建转账关系
    createCall: 创建通话关系
    personCU: person实体的增加/更新
    txCU: transaction实体的增加/更新
    phoneCU: phone实体的增加/更新
    
'''


class GraphCU:
    def __init__(self):
        self.graph = Graph(url, username=username, password=password)
    '''
        创建person实体间的关系
        Input: rel must in ['COLLEAGUE_OF', 'ACKNOWLEDGE', 'PARENT_OF']
        Return: 创建成功返回1 否则返回-1
    '''
    def createRel(self, value1, value2, rel):
        relTypes = ['COLLEAGUE_OF', 'ACKNOWLEDGE', 'PARENT_OF']
        if rel in relTypes:
            if len(self.graph.run(
                    "match(a:person{personID:'" + value1 + "'}),(b:person{personID:'" + value2 + "'})return a,b").data()) > 0:
                self.graph.run(
                    "match (s:`person`{personID:'" + value1 + "'}),(o:`person`{personID:'" + value2 + "'}) merge (s)-[:" + rel + "]->(o) return 1")
                return 1
            else:
                return -1
        else:
            return -1

    '''
        创建两个personID间的转账关系
        Input：amount.type 为 int
        Return: 创建成功返回1 否则返回-1
    '''

    def createRmt(self, value1, value2, amount):
        if str(type(amount)) == "<class 'int'>" and amount > 0:
            if len(self.graph.run(
                    "match(a:person{personID:'" + value1 + "'}),(b:person{personID:'" + value2 + "'})return a,b").data()) > 0:
                self.graph.run(
                    "match (s:`person`{personID:'" + value1 + "'}),(o:`person`{personID:'" + value2 + "'}) merge (s)-[:remittance{amount:'" + str(
                        amount) + "'}]->(o) return 1")
                return 1
            else:
                return -1
        else:
            return -1

    '''
        创建两个personID间的通话关系
        Return: 创建成功返回1 否则返回-1
    '''

    def createCall(self, value1, value2):
        if len(self.graph.run(
                "match(a:person{personID:'" + value1 + "'}),(b:person{personID:'" + value2 + "'})return a,b").data()) > 0:
            self.graph.run(
                "match (s:`person`{personID:'" + value1 + "'}),(o:`person`{personID:'" + value2 + "'}) merge (s)-[:call]->(o) return 1")
            return 1
        else:
            return -1

    '''
        person实体的增加/更新
        Input:data (list, 属性可以不全)
                    dataExample: data = {'address':'广州','age':27,'flag':0,'name':'name_1','personID':'jeanne'}
              mode ('UPDATE'/'CREATE')
        Return: 创建成功返回1 否则返回-1
    '''

    def personCU(self, data, mode='UPDATE'):
        try:
            # CQL: SET
            cache = ''
            for e in data:
                cache += " set a." + e + "='" + str(data[e]) + "'"
            if mode == 'UPDATE':
                if len(self.graph.run("match(a:person{personID:'" + data['personID'] + "'}) return a").data()) > 0:
                    self.graph.run("match(a:person{personID:'" + data['personID'] + "'})" + cache)
                    return 1
                return -1
            elif mode == 'CREATE':
                if len(self.graph.run("match(a:person{personID:'" + data['personID'] + "'}) return a").data()) == 0:
                    self.graph.run("create(a:person)" + cache)
                    return 1
                return -1
            else:
                return -1
        except:
            return -1
    '''
        transaction实体的增加/更新
        Input:data (list, 属性可以不全)
                    dataExample: data = {'status':'CLEAR','txID':'tx_1t'}
              mode ('UPDATE'/'CREATE')
        Return: 创建成功返回1 否则返回-1
    '''
    def txCU(self, data, mode='UPDATE'):
        try:
            # CQL: SET
            cache = ''
            for e in data:
                cache += " set a." + e + "='" + str(data[e]) + "'"
            if mode == 'UPDATE':
                if len(self.graph.run("match(a:transaction{txID:'" + data['txID'] + "'}) return a").data()) > 0:
                    self.graph.run("match(a:transaction{txID:'" + data['txID'] + "'})" + cache)
                    return 1
                return -1
            elif mode == 'CREATE':
                # 不存在才创建
                if len(self.graph.run("match(a:transaction{txID:'" + data['txID'] + "'}) return a").data()) == 0:
                    self.graph.run("create(a:transaction)" + cache)
                    return 1
                return -1
            else:
                return -1
        except:
            return -1
    '''
        phone实体的增加/更新
        Input:data (list, 属性可以不全)
                    dataExample: data = {'flag':0,'number':'7777','phone':'phone_1t'}
              mode ('UPDATE'/'CREATE')
        Return: 创建成功返回1 否则返回-1
    '''
    def phoneCU(self, data, mode='UPDATE'):
        try:
            # CQL: SET
            cache = ''
            for e in data:
                cache += " set a." + e + "='" + str(data[e]) + "'"
            if mode == 'UPDATE':
                if len(self.graph.run("match(a:phone{phone:'" + data['phone'] + "'}) return a").data()) > 0:
                    self.graph.run("match(a:phone{phone:'" + data['phone'] + "'})" + cache)
                    return 1
                return -1
            elif mode == 'CREATE':
                # 不存在才创建
                if len(self.graph.run("match(a:phone{phone:'" + data['phone'] + "'}) return a").data()) == 0:
                    self.graph.run("create(a:phone)" + cache)
                    return 1
                return -1
            else:
                return -1
        except:
            return -1
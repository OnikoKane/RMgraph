from py2neo import Graph

url = 'localhost:7474'
username = 'neo4j'
password = 'neo4j'

'''
    networkSearch: 实现userRel的辅助func
    userRel: 查询与传入personID有关系的user(查询两层关系)
    userPhone: 查询user所拥有的phone
    userTx: 查询user的历史贷款信息
'''

class GraphService:
    def __init__(self):
        self.graph = Graph(url, username=username, password=password)

    '''
        辅助func
        Input: personID
    '''

    def networkSearch(self, value):
        return {
            'colleague': self.graph.run(
                "match (s:`person`)-[p:`COLLEAGUE_OF`]-(o:`person`) where s.personID= '{}' return o.name,o.personID, o.status, o.flag".format(
                    value)).data(),
            'acknowledge': self.graph.run(
                "match (s:`person`)-[p:`ACKNOWLEDGE`]-(o:`person`) where s.personID= '{}' return o.name,o.personID, o.status, o.flag".format(
                    value)).data(),
            'parent': self.graph.run(
                "match (s:`person`)-[p:`PARENT_OF`]-(o:`person`) where s.personID= '{}' return o.name,o.personID, o.status, o.flag".format(
                    value)).data(),
        }

    '''
        搜索用户两层内的关系网
        SPO: S为搜索的user，P为rel，O为相关联user实体
        Input: personID (避免重名)
        Return: dict(ID存在）
               -1（ID不存在）
    '''

    def userRel(self, value):
        userEntity = self.graph.run(
            "match (s:`person`) where s.personID= '{}' return s.name, s.personID, s.status, s.flag".format(value)).data()
        if len(userEntity) > 0:  # 判断ID是否存在
            rt = {}
            rt['rels'] = self.networkSearch(value)
            rt['user'] = userEntity
            relTypes = list(rt['rels'].keys())
            for relType in relTypes:
                for O in rt['rels'][relType]:
                    O['rels'] = self.networkSearch(value=O['o.personID'])
            return rt
        else:
            return -1

    '''
        搜索用户的手机
        Input: personID (避免重名)
        Return: dict(ID存在）
               -1（ID不存在）
    '''

    def userPhone(self, value):
        userEntity = self.graph.run(
            "match (:`person`) where s.personID= '{}' return s.name, s.personID, s.status, s.flag".format(value)).data()
        if len(userEntity) > 0:
            rt = {}
            rt['user'] = userEntity
            rt['ownPhone'] = self.graph.run(
                "match (s:`person`)-[p:`ownPhone`]-(o：`phone`) where s.personID= '{}' return o.number,o.phone,o.flag".format(
                    value)).data()
            return rt
        else:
            return -1

    '''
        搜索用户的历史交易
        Input: personID (避免重名)
        Return: dict(ID存在）
               -1（ID不存在）
    '''

    def userTx(self, value):
        userEntity = self.graph.run(
            "match (:`person`) where s.personID= '{}' return s.name, s.personID, s.status, s.flag".format(value)).data()
        if len(userEntity) > 0:
            rt = {}
            rt['user'] = userEntity
            rt['apply'] = self.graph.run(
                "match (s:`person`)-[p:`apply`]-(o:`transaction`) where s.personID= '{}' return o.txID,o.status".format(value)).data()
            return rt
        else:
            return -1

from py2neo import Graph

url = 'http://localhost:7474'
username = 'neo4j'
password = 'neo4j'
'''
    一系列辅助func，不需要使用
    statExData: 图数据库统计数据
    userRel: 查询与传入personID有关系的user(查询两层关系)
    userPhone: 查询user所拥有的phone
    userTx: 查询user的历史贷款信息
'''


class GraphService:
    def __init__(self):
        self.graph = Graph(url, username=username, password=password)

    '''
        ###### 辅助func ######
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
            ##### 以下方法用于生成隐藏数据 ##### 
            ##### 也可以作为前端使用 ##### 
        '''
    '''
        返回用户的flag和status
        status：rank D -> rank AA 
    '''

    def userInfo(self, value):
        try:
            rt = self.graph.run("match (s:`person`) where s.personID= '{}' return s.flag,s.status".format(value)).data()
            return (rt[0]['s.flag'], rt[0]['s.status'])
        except:
            return -1

    '''
        一层关系黑户人数
    '''

    def network1FlagNum(self, value):
        return len(self.graph.run(
            "match (s:`person`)-[p]-(o:`person`) where type(p) in ['PARENT_OF','ACKNOWLEDGE','COLLEAGUE_OF'] and o.flag='1' and s.personID= '{}' return o.personID".format(
                value)).data())

    '''
        二层关系黑户人数
    '''

    def network2FlagNum(self, value):
        return len(self.graph.run(
            "match (s:`person`)-[p]-(:`person`)-[q]-(o:`person`) where type(p) in ['PARENT_OF','ACKNOWLEDGE','COLLEAGUE_OF'] and type(q) in ['PARENT_OF','ACKNOWLEDGE','COLLEAGUE_OF'] and o.flag='1' and s.personID= '{}' return o.personID".format(
                value)).data())

    '''
        用户持有电话数量
    '''

    def userPhoneNum(self, value):
        return len(self.graph.run(
            "match (s:`person`)-[p:`ownPhone`]-(o:`phone`) where s.personID= '{}' return o.phone".format(value)).data())

    '''
        用户转账数目(金额>=4000)
    '''

    def userRmtNum(self, value):
        # 交易金额大于4000才计数
        # try:
        #     cache = self.graph.run(
        #         "match (s:`person`)-[p:`remittance`]-(o:`person`) where s.personID= '{}' return p.amount".format(
        #             value)).data()
        #     return len([i['p.amount'] for i in cache if int(i['p.amount']) >= 4000])
        # except:
        #     return -1
        return len(self.graph.run(
            "match (s:`person`)-[p:`remittance`]-(o:`person`) where s.personID= '{}' and toInt(p.amount)>=4000 return p.amount".format(
                value)).data())

    '''
        返回用户tx的数目x3(违约、完结、被拒）
    '''

    def userTxConditionNum(self, value):
        defaultNum = len(self.graph.run(
            "match (s:`person`)-[p:`apply`]-(o:`transaction`) where o.status='OVER_DUE' and s.personID= '{}' return o.status".format(
                value)).data())
        clearNum = len(self.graph.run(
            "match (s:`person`)-[p:`apply`]-(o:`transaction`) where o.status='CLEAR' and s.personID= '{}' return o.status".format(
                value)).data())
        rejectedNum = len(self.graph.run(
            "match (s:`person`)-[p:`apply`]-(o:`transaction`) where o.status='REJECT' and s.personID= '{}' return o.status".format(
                value)).data())
        return defaultNum, clearNum, rejectedNum

    '''
        ############ 以上是辅助函数 ############
        #######################################
    '''

    '''
        neo4j统计得到的额外数据
        Input: personID
        Output: dict / -1
    '''
    def statExData(self, value):
        try:
            i = self.userInfo(value=value)
            c1, c2, c3 = self.userTxConditionNum(value=value)
            return {
                '是否在黑名单': i[0],
                '历史用户评分': i[1],
                '一层关系黑户数': self.network1FlagNum(value=value),
                '二层关系黑户数': self.network2FlagNum(value=value),
                '持有手机数': self.userPhoneNum(value=value),
                '大额转账数': self.userRmtNum(value=value),
                '违约贷款数': c1,
                '完结贷款数': c2,
                '被拒贷款数': c3,
            }
        except:
            return -1


    '''
        搜索用户两层内的关系网
        SPO: S为搜索的user，P为rel，O为相关联user实体
        Input: personID (避免重名)
        Return: dict(ID存在）
               -1（ID不存在）
    '''

    def userRel(self, value):
        userEntity = self.graph.run(
            "match (s:`person`) where s.personID= '{}' return s.name, s.personID, s.status, s.flag".format(
                value)).data()
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
            "match (s:`person`) where s.personID= '{}' return s.name, s.personID, s.status, s.flag".format(value)).data()
        if len(userEntity) > 0:
            rt = {}
            rt['user'] = userEntity
            rt['ownPhone'] = self.graph.run(
                "match (s:`person`)-[p:`ownPhone`]-(o:`phone`) where s.personID= '{}' return o.number,o.phone,o.flag".format(
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
            "match (s:`person`) where s.personID= '{}' return s.name, s.personID, s.status, s.flag".format(value)).data()
        if len(userEntity) > 0:
            rt = {}
            rt['user'] = userEntity
            rt['apply'] = self.graph.run(
                "match (s:`person`)-[p:`apply`]-(o:`transaction`) where s.personID= '{}' return o.txID,o.status".format(
                    value)).data()
            return rt
        else:
            return -1
